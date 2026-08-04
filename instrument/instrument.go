// Package instrument provides shared OpenTelemetry tracing and optional SQL query logging for the driver-specific instrument/postgres and instrument/sqlite packages
package instrument

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/italypaleale/go-sql-utils/instrument"

// Options configures SQL instrumentation
type Options struct {
	// System overrides the database system name
	// Driver-specific packages use "sqlite" and "postgresql" when this is empty
	System string

	// Log receives query and slow-operation logs
	// Nil disables logging
	Log *slog.Logger

	// QueryLog enables Debug logs for completed SQL statements
	QueryLog bool

	// IncludeParameters includes query parameter values in traces and in SQL logs that include statement text
	// Parameter values may contain sensitive information and are excluded by default
	IncludeParameters bool

	// SlowThreshold emits Warn logs for queries and selected database operations at or above this duration
	// Zero disables slow-operation logs
	SlowThreshold time.Duration
}

// SystemName returns the configured database system, or fallback when the options are nil or the configured value is empty
func (o *Options) SystemName(fallback string) string {
	if o == nil || o.System == "" {
		return fallback
	}

	return o.System
}

// Logger returns the configured logger
// Nil means logging is disabled
func (o *Options) Logger() *slog.Logger {
	if o == nil {
		return nil
	}

	return o.Log
}

// QueryLoggingEnabled reports whether per-statement Debug logging is enabled
func (o *Options) QueryLoggingEnabled() bool {
	return o != nil && o.QueryLog
}

// QueryParametersIncluded reports whether query parameter values are included in logs and traces
func (o *Options) QueryParametersIncluded() bool {
	return o != nil && o.IncludeParameters
}

// SlowQueryThreshold returns the configured slow-operation threshold
// Values <= 0 (default) disable slow-operation logging
func (o *Options) SlowQueryThreshold() time.Duration {
	if o == nil || o.SlowThreshold <= 0 {
		return 0
	}

	return o.SlowThreshold
}

// Instrumentation is an immutable, concurrency-safe snapshot of Options for a database system
type Instrumentation struct {
	system            string
	log               *slog.Logger
	queryLog          bool
	includeParameters bool
	slowThreshold     time.Duration
	tracer            trace.Tracer
}

// QueryParameter is a named or positional value bound to a SQL statement
type QueryParameter struct {
	Name  string
	Value any
}

type querySource struct {
	file string
	line int
}

type querySourceContextKey struct{}

// NewInstrumentation resolves opts for system
// opts may be nil
func NewInstrumentation(system string, opts *Options) *Instrumentation {
	return &Instrumentation{
		system:            opts.SystemName(system),
		log:               opts.Logger(),
		queryLog:          opts.QueryLoggingEnabled(),
		includeParameters: opts.QueryParametersIncluded(),
		slowThreshold:     opts.SlowQueryThreshold(),
		tracer:            otel.Tracer(instrumentationName, trace.WithSchemaURL(semconv.SchemaURL)),
	}
}

// StartSpan starts a client span for a database operation
// Full SQL text is attached to spans, but parameters are excluded unless IncludeParameters is enabled
//
//nolint:spancheck // The span is returned and ended by the matching driver callback
func (i *Instrumentation) StartSpan(ctx context.Context, op, statement string) (context.Context, trace.Span) {
	return i.StartQuerySpan(ctx, op, statement, nil)
}

// StartQuerySpan starts a client span for a database operation with its bound parameters
// Parameter values may contain sensitive information and are attached only when IncludeParameters is enabled
//
//nolint:spancheck // The span is returned and ended by the matching driver callback
func (i *Instrumentation) StartQuerySpan(ctx context.Context, op, statement string, parameters []QueryParameter) (context.Context, trace.Span) {
	// Build the database attributes once so the span starts with complete query metadata
	size := 3
	if i.includeParameters {
		size += len(parameters)
	}
	dbAttrs := make([]attribute.KeyValue, 0, size)
	dbAttrs = append(dbAttrs,
		semconv.DBSystemNameKey.String(i.system),
		semconv.DBOperationName(op),
	)
	if statement != "" {
		dbAttrs = append(dbAttrs, semconv.DBQueryText(statement))
	}
	if i.includeParameters {
		dbAttrs = append(dbAttrs, traceParameterAttrs(parameters)...)
	}

	// Start a client span with the query metadata already attached
	attrs := make([]trace.SpanStartOption, 0, 3)
	attrs = append(attrs,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(dbAttrs...),
	)

	// Retain the application call site only when a later query log may need it
	spanCtx, span := i.tracer.Start(ctx, i.system+"."+op, attrs...)
	if i.log != nil && (i.queryLog || i.slowThreshold > 0) {
		source := captureQuerySource()
		if source.file != "" {
			spanCtx = context.WithValue(spanCtx, querySourceContextKey{}, source)
		}
	}

	return spanCtx, span
}

// EndSpan records err, when non-nil, and ends span
func EndSpan(span trace.Span, err error) {
	// Preserve failure details before the span becomes immutable
	if err != nil {
		FailSpan(span, err)
	}

	// Finish the span after all outcome metadata has been recorded
	span.End()
}

// FailSpan records err on span and marks it failed without ending it
func FailSpan(span trace.Span, err error) {
	// A nil error leaves the existing span state untouched
	if err == nil {
		return
	}

	// Record both the error event and failed status for different telemetry consumers
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// EmitQueryLog emits a Debug record for a completed SQL statement or a Warn record when it exceeded the slow threshold
// Normalized statement text is included only when Debug query logging is active
func (i *Instrumentation) EmitQueryLog(ctx context.Context, op, statement string, dur time.Duration, err error) {
	i.EmitQueryLogWithParameters(ctx, op, statement, nil, dur, err)
}

// EmitQueryLogWithParameters emits a completed SQL statement log with its parameters when configured
func (i *Instrumentation) EmitQueryLogWithParameters(ctx context.Context, op, statement string, parameters []QueryParameter, dur time.Duration, err error) {
	// Avoid log-level and metadata work when no logger can consume the record
	if i.log == nil {
		return
	}

	// Resolve whether normal or slow-query policy requires a record
	debug := i.debugEnabled(ctx)
	slow := i.slowThreshold > 0 && dur >= i.slowThreshold
	if !slow && !debug {
		return
	}

	// Slow-query severity takes precedence when both policies match
	level := slog.LevelDebug
	msg := "Executed SQL statement"
	if slow {
		level = slog.LevelWarn
		msg = "Slow SQL statement"
	}

	// Attach the call site and include query details only when Debug query logging is active
	attrs := i.logAttrs(op, dur, err)
	source := querySourceFromContext(ctx)
	if source.file != "" {
		attrs = append(attrs,
			slog.String("code.file.path", source.file),
			slog.Int("code.line.number", source.line),
		)
	}
	if debug && statement != "" {
		attrs = append(attrs, slog.String("db.query.text", normalizeStatement(statement)))
	}
	if debug && i.includeParameters {
		attrs = append(attrs, logParameterAttrs(parameters)...)
	}

	// Emit after the full record has been assembled so every handler sees consistent metadata
	i.log.LogAttrs(ctx, level, msg, attrs...)
}

// EmitSlowOperationLog emits a Warn record for a non-statement database operation when it exceeded the configured slow threshold
func (i *Instrumentation) EmitSlowOperationLog(ctx context.Context, op string, dur time.Duration, err error) {
	// Non-statement operations are logged only when they cross the configured threshold
	if i.log == nil || i.slowThreshold <= 0 || dur < i.slowThreshold {
		return
	}

	// Reuse the common database attributes so statement and operation warnings remain consistent
	i.log.LogAttrs(ctx, slog.LevelWarn, "Slow database operation", i.logAttrs(op, dur, err)...)
}

// AddQueryEvent adds a statement event to a larger database operation, such as a pgx batch
// Exact SQL text is attached to traces independently from logging
func (i *Instrumentation) AddQueryEvent(ctx context.Context, span trace.Span, statement string) {
	i.AddQueryEventWithParameters(ctx, span, statement, nil)
}

// AddQueryEventWithParameters adds a statement event and its configured parameters to a larger database operation
func (i *Instrumentation) AddQueryEventWithParameters(ctx context.Context, span trace.Span, statement string, parameters []QueryParameter) {
	// Build one attribute set so batch events follow the same parameter policy as query spans
	options := make([]trace.EventOption, 0, 1)
	eventAttrs := make([]attribute.KeyValue, 0, 1+len(parameters))
	if statement != "" {
		eventAttrs = append(eventAttrs, semconv.DBQueryText(statement))
	}
	if i.includeParameters {
		eventAttrs = append(eventAttrs, traceParameterAttrs(parameters)...)
	}
	if len(eventAttrs) > 0 {
		options = append(options, trace.WithAttributes(eventAttrs...))
	}

	// Preserve the query occurrence even when it carries no attributes
	span.AddEvent("query", options...)
}

func (i *Instrumentation) debugEnabled(ctx context.Context) bool {
	return i.log != nil && i.queryLog && i.log.Enabled(ctx, slog.LevelDebug)
}

func (i *Instrumentation) logAttrs(op string, dur time.Duration, err error) []slog.Attr {
	// Keep the shared database identity and timing fields stable across every log policy
	attrs := []slog.Attr{
		slog.String("db.system.name", i.system),
		slog.String("db.operation.name", op),
		slog.Duration("duration", dur),
	}

	// Include failures without changing the shape of successful records
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}

	return attrs
}

func normalizeStatement(statement string) string {
	// Reserve the input length because normalization can only shorten the statement
	var normalized strings.Builder
	normalized.Grow(len(statement))

	// Delay internal spaces until the next non-space rune so leading and trailing whitespace disappear
	pendingSpace := false
	wroteText := false
	for _, char := range statement {
		if unicode.IsSpace(char) {
			pendingSpace = wroteText
			continue
		}
		if pendingSpace {
			normalized.WriteByte(' ')
			pendingSpace = false
		}
		normalized.WriteRune(char)
		wroteText = true
	}

	return normalized.String()
}

func traceParameterAttrs(parameters []QueryParameter) []attribute.KeyValue {
	// Convert values to the string representation required by OpenTelemetry database conventions
	attrs := make([]attribute.KeyValue, 0, len(parameters))
	for index, parameter := range parameters {
		name := parameterName(parameter.Name, index)
		attrs = append(attrs, semconv.DBQueryParameter(name, fmt.Sprint(parameter.Value)))
	}

	return attrs
}

func logParameterAttrs(parameters []QueryParameter) []slog.Attr {
	// Preserve native values in structured logs while sharing the trace attribute key format
	attrs := make([]slog.Attr, 0, len(parameters))
	for index, parameter := range parameters {
		name := parameterName(parameter.Name, index)
		attrs = append(attrs, slog.Any("db.query.parameter."+name, parameter.Value))
	}

	return attrs
}

func parameterName(name string, index int) string {
	if name != "" {
		return name
	}

	return strconv.Itoa(index + 1)
}

func captureQuerySource() querySource {
	// Capture enough of the synchronous database stack to reach the application query call
	pcs := make([]uintptr, 32)
	count := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:count])

	// Return the first frame outside runtime, driver, and instrumentation plumbing
	for {
		frame, more := frames.Next()
		if !isInternalFrame(frame) {
			return querySource{
				file: filepath.Base(frame.File),
				line: frame.Line,
			}
		}
		if !more {
			return querySource{}
		}
	}
}

func isInternalFrame(frame runtime.Frame) bool {
	// Runtime and reflection frames never identify the application query
	if strings.HasPrefix(frame.Function, "runtime.") || strings.HasPrefix(frame.Function, "reflect.") {
		return true
	}

	// Database and pgx frames sit between the instrumentation callback and its caller
	if strings.HasPrefix(frame.Function, "database/sql.") || strings.HasPrefix(frame.Function, "database/sql/driver.") {
		return true
	}
	if strings.HasPrefix(frame.Function, "github.com/jackc/pgx/v5") {
		return true
	}

	// Instrumentation implementation frames are skipped while test callers remain observable
	if strings.HasPrefix(frame.Function, instrumentationName) && !strings.HasSuffix(frame.File, "_test.go") {
		return true
	}

	return false
}

func querySourceFromContext(ctx context.Context) querySource {
	source, ok := ctx.Value(querySourceContextKey{}).(querySource)
	if !ok {
		return querySource{}
	}

	return source
}
