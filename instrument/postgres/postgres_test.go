package postgres

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/italypaleale/go-sql-utils/instrument"
)

// Compile-time checks for the full set of pgx and pgxpool tracing interfaces
var (
	_ pgx.QueryTracer       = (*pgxTracer)(nil)
	_ pgx.BatchTracer       = (*pgxTracer)(nil)
	_ pgx.CopyFromTracer    = (*pgxTracer)(nil)
	_ pgx.PrepareTracer     = (*pgxTracer)(nil)
	_ pgx.ConnectTracer     = (*pgxTracer)(nil)
	_ pgxpool.AcquireTracer = (*pgxTracer)(nil)
	_ pgxpool.ReleaseTracer = (*pgxTracer)(nil)
)

// setupSpanRecorder records spans during a test and restores the previous provider during cleanup
func setupSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	prev := otel.GetTracerProvider()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
	})

	return sr
}

// spansByName indexes the recorded spans by name
func spansByName(t *testing.T, sr *tracetest.SpanRecorder) map[string]sdktrace.ReadOnlySpan {
	t.Helper()

	byName := make(map[string]sdktrace.ReadOnlySpan)
	for _, s := range sr.Ended() {
		byName[s.Name()] = s
	}

	return byName
}

// spanAttr returns the value of the span attribute with the given key, reporting whether it was present
func spanAttr(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.AsString(), true
		}
	}

	return "", false
}

// captureHandler is a slog.Handler that collects records in memory for assertions
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
	level   slog.Level
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{
		level: slog.LevelDebug,
	}
}

func (h *captureHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h
}

// attrStrings returns all attribute values of the record as strings, for "must not contain parameter values" assertions
func attrStrings(r slog.Record) []string {
	vals := make([]string, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		vals = append(vals, a.Value.String())
		return true
	})

	return vals
}

func recordAttr(record slog.Record, key string) (slog.Value, bool) {
	var value slog.Value
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key {
			value = attr.Value
			found = true
		}
		return true
	})

	return value, found
}

// fakePgxTracer is a chained tracer implementing every pgx tracing interface, recording which callbacks it received
type fakePgxTracer struct {
	calls []string
}

func (f *fakePgxTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	f.calls = append(f.calls, "queryStart")
	return ctx
}

func (f *fakePgxTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	f.calls = append(f.calls, "queryEnd")
}

func (f *fakePgxTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	f.calls = append(f.calls, "batchStart")
	return ctx
}

func (f *fakePgxTracer) TraceBatchQuery(_ context.Context, _ *pgx.Conn, _ pgx.TraceBatchQueryData) {
	f.calls = append(f.calls, "batchQuery")
}

func (f *fakePgxTracer) TraceBatchEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceBatchEndData) {
	f.calls = append(f.calls, "batchEnd")
}

func (f *fakePgxTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	f.calls = append(f.calls, "copyFromStart")
	return ctx
}

func (f *fakePgxTracer) TraceCopyFromEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromEndData) {
	f.calls = append(f.calls, "copyFromEnd")
}

func (f *fakePgxTracer) TracePrepareStart(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareStartData) context.Context {
	f.calls = append(f.calls, "prepareStart")
	return ctx
}

func (f *fakePgxTracer) TracePrepareEnd(_ context.Context, _ *pgx.Conn, _ pgx.TracePrepareEndData) {
	f.calls = append(f.calls, "prepareEnd")
}

func (f *fakePgxTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	f.calls = append(f.calls, "connectStart")
	return ctx
}

func (f *fakePgxTracer) TraceConnectEnd(_ context.Context, _ pgx.TraceConnectEndData) {
	f.calls = append(f.calls, "connectEnd")
}

func TestPgxTracerEmitsQuerySpansWithoutArgs(t *testing.T) {
	sr := setupSpanRecorder(t)

	tracer := NewTracer(&instrument.Options{
		Log:      slog.New(newCaptureHandler()),
		QueryLog: true,
	})

	// Drive the tracer directly: the composite never dereferences the connection, so a nil one is fine
	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{
		SQL:  "UPDATE hosts SET host_last_health_check = $1 WHERE host_id = $2",
		Args: []any{"secret-value-123", "param-id-xyz"},
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{
		CommandTag: pgconn.NewCommandTag("UPDATE 1"),
	})

	span := spansByName(t, sr)["postgresql.query"]
	require.NotNil(t, span, "expected a postgresql.query span")

	stmt, ok := spanAttr(span, "db.query.text")
	require.True(t, ok)
	assert.Equal(t, "UPDATE hosts SET host_last_health_check = $1 WHERE host_id = $2", stmt)
	system, ok := spanAttr(span, "db.system.name")
	require.True(t, ok)
	assert.Equal(t, "postgresql", system)
	operation, ok := spanAttr(span, "db.operation.name")
	require.True(t, ok)
	assert.Equal(t, "query", operation)
	assert.NotEqual(t, codes.Error, span.Status().Code)

	for _, attr := range span.Attributes() {
		assert.NotContains(t, attr.Value.AsString(), "secret-value-123")
		assert.NotContains(t, attr.Value.AsString(), "param-id-xyz")
	}
}

func TestPgxTracerIncludesParametersWhenEnabled(t *testing.T) {
	sr := setupSpanRecorder(t)
	handler := newCaptureHandler()
	tracer := NewTracer(&instrument.Options{
		Log:               slog.New(handler),
		QueryLog:          true,
		IncludeParameters: true,
	})

	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{
		SQL:  "UPDATE hosts SET host_last_health_check = $1 WHERE host_id = $2",
		Args: []any{"secret-value-123", "param-id-xyz"},
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	span := spansByName(t, sr)["postgresql.query"]
	require.NotNil(t, span)
	first, ok := spanAttr(span, "db.query.parameter.1")
	require.True(t, ok)
	assert.Equal(t, "secret-value-123", first)
	second, ok := spanAttr(span, "db.query.parameter.2")
	require.True(t, ok)
	assert.Equal(t, "param-id-xyz", second)

	require.Len(t, handler.records, 1)
	firstLog, ok := recordAttr(handler.records[0], "db.query.parameter.1")
	require.True(t, ok)
	assert.Equal(t, "secret-value-123", firstLog.String())
	file, ok := recordAttr(handler.records[0], "code.file.path")
	require.True(t, ok)
	assert.Equal(t, "postgres_test.go", file.String())
	line, ok := recordAttr(handler.records[0], "code.line.number")
	require.True(t, ok)
	assert.Positive(t, line.Int64())
}

func TestPgxTracerRecordsQueryError(t *testing.T) {
	sr := setupSpanRecorder(t)

	tracer := NewTracer(nil)
	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{
		Err: context.DeadlineExceeded,
	})

	span := spansByName(t, sr)["postgresql.query"]
	require.NotNil(t, span)
	assert.Equal(t, codes.Error, span.Status().Code)
}

func TestPgxTracerForwardsToChainedTracers(t *testing.T) {
	setupSpanRecorder(t)

	fake := &fakePgxTracer{}
	tracer := NewTracer(nil, nil, fake)

	// Exercise every interface; the chained tracer must see each callback even though nil entries were chained
	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	batchTracer, ok := tracer.(pgx.BatchTracer)
	require.True(t, ok, "composite must implement pgx.BatchTracer")
	ctx = batchTracer.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
	batchTracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{SQL: "SELECT 1"})
	batchTracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	copyTracer, ok := tracer.(pgx.CopyFromTracer)
	require.True(t, ok, "composite must implement pgx.CopyFromTracer")
	ctx = copyTracer.TraceCopyFromStart(t.Context(), nil, pgx.TraceCopyFromStartData{TableName: pgx.Identifier{"items"}})
	copyTracer.TraceCopyFromEnd(ctx, nil, pgx.TraceCopyFromEndData{})

	prepareTracer, ok := tracer.(pgx.PrepareTracer)
	require.True(t, ok, "composite must implement pgx.PrepareTracer")
	ctx = prepareTracer.TracePrepareStart(t.Context(), nil, pgx.TracePrepareStartData{SQL: "SELECT 1"})
	prepareTracer.TracePrepareEnd(ctx, nil, pgx.TracePrepareEndData{})

	connectTracer, ok := tracer.(pgx.ConnectTracer)
	require.True(t, ok, "composite must implement pgx.ConnectTracer")
	ctx = connectTracer.TraceConnectStart(t.Context(), pgx.TraceConnectStartData{})
	connectTracer.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

	assert.Equal(t, []string{
		"queryStart", "queryEnd",
		"batchStart", "batchQuery", "batchEnd",
		"copyFromStart", "copyFromEnd",
		"prepareStart", "prepareEnd",
		"connectStart", "connectEnd",
	}, fake.calls)
}

// queryOnlyTracer only implements pgx.QueryTracer, so chained interfaces beyond it must be skipped without panicking
type queryOnlyTracer struct {
	calls []string
}

func (q *queryOnlyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	q.calls = append(q.calls, "queryStart")
	return ctx
}

func (q *queryOnlyTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	q.calls = append(q.calls, "queryEnd")
}

func TestPgxTracerWithQueryOnlyChain(t *testing.T) {
	sr := setupSpanRecorder(t)

	queryOnly := &queryOnlyTracer{}
	tracer := NewTracer(nil, queryOnly)

	// Batch operations with no chained BatchTracer must still produce a span without panicking
	batchTracer, ok := tracer.(pgx.BatchTracer)
	require.True(t, ok)
	ctx := batchTracer.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
	batchTracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	assert.NotNil(t, spansByName(t, sr)["postgresql.batch"], "expected a postgresql.batch span")
	assert.Empty(t, queryOnly.calls, "the query-only chained tracer must not receive batch callbacks")

	// Queries are still forwarded
	ctx = tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	assert.Equal(t, []string{"queryStart", "queryEnd"}, queryOnly.calls)
}

func TestPgxTracerQueryLogAndSlowThreshold(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	tracer := NewTracer(&instrument.Options{
		Log:      slog.New(handler),
		QueryLog: true,
	})

	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{
		SQL:  "UPDATE hosts SET host_last_health_check = $1 WHERE host_id = $2",
		Args: []any{"secret-value-123", "param-id-xyz"},
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	require.Len(t, handler.records, 1)
	r := handler.records[0]
	assert.Equal(t, slog.LevelDebug, r.Level)
	for _, v := range attrStrings(r) {
		assert.NotContains(t, v, "secret-value-123")
		assert.NotContains(t, v, "param-id-xyz")
	}
}

func TestPgxTracerPreparesAreNotLogged(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	tracer := NewTracer(&instrument.Options{
		Log:      slog.New(handler),
		QueryLog: true,
	})

	prepareTracer, ok := tracer.(pgx.PrepareTracer)
	require.True(t, ok)
	ctx := prepareTracer.TracePrepareStart(t.Context(), nil, pgx.TracePrepareStartData{SQL: "SELECT 1"})
	prepareTracer.TracePrepareEnd(ctx, nil, pgx.TracePrepareEndData{})

	// Prepares emit spans but never log records, to avoid duplicating every query's log line under statement caching
	assert.Empty(t, handler.records)
}

func TestPgxTracerSlowAcquireIsWarned(t *testing.T) {
	sr := setupSpanRecorder(t)

	handler := newCaptureHandler()
	tracer := NewTracer(&instrument.Options{
		Log:           slog.New(handler),
		SlowThreshold: time.Nanosecond,
	})

	acquireTracer, ok := tracer.(pgxpool.AcquireTracer)
	require.True(t, ok, "composite must implement pgxpool.AcquireTracer")

	ctx := acquireTracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
	acquireTracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})

	// The acquisition produced a span, and its (nonzero) duration exceeded the nanosecond threshold, so it was logged at Warn
	assert.NotNil(t, spansByName(t, sr)["postgresql.pool.acquire"], "expected a postgresql.pool.acquire span")
	require.NotEmpty(t, handler.records)
	for _, r := range handler.records {
		assert.Equal(t, slog.LevelWarn, r.Level)
	}
}

func TestNestedPgxTracersKeepIndependentSpanState(t *testing.T) {
	sr := setupSpanRecorder(t)

	inner := NewTracer(nil)
	outer := NewTracer(nil, inner)
	ctx := outer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	outer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	var spans []sdktrace.ReadOnlySpan
	for _, span := range sr.Ended() {
		if span.Name() == "postgresql.query" {
			spans = append(spans, span)
		}
	}
	require.Len(t, spans, 2)
	assert.NotEqual(t, spans[0].SpanContext().SpanID(), spans[1].SpanContext().SpanID())

	var parent, child sdktrace.ReadOnlySpan
	if spans[0].Parent().SpanID() == spans[1].SpanContext().SpanID() {
		child, parent = spans[0], spans[1]
	} else {
		child, parent = spans[1], spans[0]
	}
	assert.Equal(t, parent.SpanContext().SpanID(), child.Parent().SpanID())
	assert.False(t, child.EndTime().After(parent.EndTime()), "child span must end before its parent")
}

func TestPgxBatchQueryErrorFailsBatchSpan(t *testing.T) {
	sr := setupSpanRecorder(t)

	queryTracer := NewTracer(nil)
	tracer, ok := queryTracer.(pgx.BatchTracer)
	require.True(t, ok, "composite must implement pgx.BatchTracer")

	ctx := tracer.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{
		SQL: "SELECT broken",
		Err: errors.New("scan failed"),
	})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	span := spansByName(t, sr)["postgresql.batch"]
	require.NotNil(t, span)
	assert.Equal(t, codes.Error, span.Status().Code)
}

func TestPgxAcquireDoesNotEmitQueryLog(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	queryTracer := NewTracer(&instrument.Options{
		Log:      slog.New(handler),
		QueryLog: true,
	})
	tracer, ok := queryTracer.(pgxpool.AcquireTracer)
	require.True(t, ok, "composite must implement pgxpool.AcquireTracer")

	ctx := tracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})
	assert.Empty(t, handler.records)
}

func TestPgxTracerOwnsChainSlice(t *testing.T) {
	setupSpanRecorder(t)

	first := &fakePgxTracer{}
	second := &fakePgxTracer{}
	chain := []pgx.QueryTracer{first}
	tracer := NewTracer(nil, chain...)
	chain[0] = second

	ctx := tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	assert.Equal(t, []string{"queryStart", "queryEnd"}, first.calls)
	assert.Empty(t, second.calls)
}

func TestPgxBatchLogsEachStatementWithoutBlankAggregate(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	queryTracer := NewTracer(&instrument.Options{
		Log:      slog.New(handler),
		QueryLog: true,
	})
	tracer, ok := queryTracer.(pgx.BatchTracer)
	require.True(t, ok, "composite must implement pgx.BatchTracer")

	ctx := tracer.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{SQL: "SELECT 1"})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{SQL: "SELECT 2"})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	require.Len(t, handler.records, 2)
	for i, want := range []string{"SELECT 1", "SELECT 2"} {
		var got string
		handler.records[i].Attrs(func(attr slog.Attr) bool {
			if attr.Key == "db.query.text" {
				got = attr.Value.String()
			}
			return true
		})
		assert.Equal(t, want, got)
	}
}

func TestPgxBatchIncludesNamedParametersInEventsAndLogs(t *testing.T) {
	sr := setupSpanRecorder(t)
	handler := newCaptureHandler()
	queryTracer := NewTracer(&instrument.Options{
		Log:               slog.New(handler),
		QueryLog:          true,
		IncludeParameters: true,
	})
	tracer, ok := queryTracer.(pgx.BatchTracer)
	require.True(t, ok)

	ctx := tracer.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{
		SQL: "SELECT @account_id",
		Args: []any{pgx.NamedArgs{
			"account_id": "secret-value",
		}},
	})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	span := spansByName(t, sr)["postgresql.batch"]
	require.NotNil(t, span)
	require.Len(t, span.Events(), 1)
	eventParameter := ""
	for _, attr := range span.Events()[0].Attributes {
		if string(attr.Key) == "db.query.parameter.account_id" {
			eventParameter = attr.Value.AsString()
		}
	}
	assert.Equal(t, "secret-value", eventParameter)

	require.Len(t, handler.records, 1)
	logParameter, ok := recordAttr(handler.records[0], "db.query.parameter.account_id")
	require.True(t, ok)
	assert.Equal(t, "secret-value", logParameter.String())
}
