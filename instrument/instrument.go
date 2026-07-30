// Package instrument provides shared OpenTelemetry tracing and optional SQL query logging for the driver-specific instrument/postgres and instrument/sqlite packages
package instrument

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
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
	// Statement text is also attached to spans only while this option is enabled and the logger's Debug level is active
	QueryLog bool

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
	system        string
	log           *slog.Logger
	queryLog      bool
	slowThreshold time.Duration
}

// NewInstrumentation resolves opts for system
// opts may be nil
func NewInstrumentation(system string, opts *Options) *Instrumentation {
	return &Instrumentation{
		system:        opts.SystemName(system),
		log:           opts.Logger(),
		queryLog:      opts.QueryLoggingEnabled(),
		slowThreshold: opts.SlowQueryThreshold(),
	}
}

// StartSpan starts a client span for a database operation
// SQL text is attached only when per-query Debug logging is active
// Bound argument values are never supplied to this package
//
//nolint:spancheck // The span is returned and ended by the matching driver callback
func (i *Instrumentation) StartSpan(ctx context.Context, op, statement string) (context.Context, trace.Span) {
	attrs := make([]trace.SpanStartOption, 0, 3)
	attrs = append(attrs,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemNameKey.String(i.system),
			semconv.DBOperationName(op),
		),
	)
	if i.debugEnabled(ctx) && statement != "" {
		attrs = append(attrs, trace.WithAttributes(semconv.DBQueryText(statement)))
	}

	tracer := otel.Tracer(instrumentationName, trace.WithSchemaURL(semconv.SchemaURL))
	return tracer.Start(ctx, i.system+"."+op, attrs...)
}

// EndSpan records err, when non-nil, and ends span
func EndSpan(span trace.Span, err error) {
	if err != nil {
		FailSpan(span, err)
	}

	span.End()
}

// FailSpan records err on span and marks it failed without ending it
func FailSpan(span trace.Span, err error) {
	if err == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// EmitQueryLog emits a Debug record for a completed SQL statement or a Warn record when it exceeded the slow threshold
// Exact statement text is included only when Debug query logging is active
func (i *Instrumentation) EmitQueryLog(ctx context.Context, op, statement string, dur time.Duration, err error) {
	if i.log == nil {
		return
	}

	debug := i.debugEnabled(ctx)
	slow := i.slowThreshold > 0 && dur >= i.slowThreshold
	if !slow && !debug {
		return
	}

	level := slog.LevelDebug
	msg := "Executed SQL statement"
	if slow {
		level = slog.LevelWarn
		msg = "Slow SQL statement"
	}

	attrs := i.logAttrs(op, dur, err)
	if debug && statement != "" {
		attrs = append(attrs, slog.String("db.query.text", statement))
	}

	i.log.LogAttrs(ctx, level, msg, attrs...)
}

// EmitSlowOperationLog emits a Warn record for a non-statement database operation when it exceeded the configured slow threshold
func (i *Instrumentation) EmitSlowOperationLog(ctx context.Context, op string, dur time.Duration, err error) {
	if i.log == nil || i.slowThreshold <= 0 || dur < i.slowThreshold {
		return
	}

	i.log.LogAttrs(ctx, slog.LevelWarn, "Slow database operation", i.logAttrs(op, dur, err)...)
}

// AddQueryEvent adds a statement event to a larger database operation, such as a pgx batch
// Exact SQL text follows the same Debug-level gate as query logs
func (i *Instrumentation) AddQueryEvent(ctx context.Context, span trace.Span, statement string) {
	options := make([]trace.EventOption, 0, 1)
	if i.debugEnabled(ctx) && statement != "" {
		options = append(options, trace.WithAttributes(semconv.DBQueryText(statement)))
	}
	span.AddEvent("query", options...)
}

func (i *Instrumentation) debugEnabled(ctx context.Context) bool {
	return i.log != nil && i.queryLog && i.log.Enabled(ctx, slog.LevelDebug)
}

func (i *Instrumentation) logAttrs(op string, dur time.Duration, err error) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("db.system.name", i.system),
		slog.String("db.operation.name", op),
		slog.Duration("duration", dur),
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}

	return attrs
}
