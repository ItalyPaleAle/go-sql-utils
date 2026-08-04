package instrument

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type testHandler struct {
	mu      sync.Mutex
	level   slog.Level
	records []slog.Record
}

func (h *testHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *testHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *testHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *testHandler) WithGroup(string) slog.Handler {
	return h
}

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	previous := otel.GetTracerProvider()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
	})

	return recorder
}

func readSpanAttr(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.AsString(), true
		}
	}
	return "", false
}

func readRecordAttr(record slog.Record, key string) (slog.Value, bool) {
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

func TestNilOptionsUseZeroValueDefaults(t *testing.T) {
	var opts *Options
	assert.Equal(t, "sqlite", opts.SystemName("sqlite"))
	assert.Nil(t, opts.Logger())
	assert.False(t, opts.QueryLoggingEnabled())
	assert.False(t, opts.QueryParametersIncluded())
	assert.Zero(t, opts.SlowQueryThreshold())
}

func TestNonPositiveSlowThresholdIsDisabled(t *testing.T) {
	assert.Zero(t, (&Options{SlowThreshold: -time.Second}).SlowQueryThreshold())
}

func TestInstrumentationSnapshotsOptions(t *testing.T) {
	recorder := recordSpans(t)
	firstHandler := &testHandler{level: slog.LevelDebug}
	secondHandler := &testHandler{level: slog.LevelDebug}
	opts := &Options{
		System:            "custom-db",
		Log:               slog.New(firstHandler),
		QueryLog:          true,
		IncludeParameters: true,
	}
	instrumentation := NewInstrumentation("fallback", opts)

	// Mutating the source after construction must not affect active calls
	opts.System = "mutated"
	opts.Log = slog.New(secondHandler)
	opts.QueryLog = false
	opts.IncludeParameters = false

	ctx, span := instrumentation.StartQuerySpan(t.Context(), "query", "SELECT $1", []QueryParameter{{Name: "1", Value: "value"}})
	EndSpan(span, nil)
	instrumentation.EmitQueryLogWithParameters(ctx, "query", "SELECT $1", []QueryParameter{{Name: "1", Value: "value"}}, time.Millisecond, nil)

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "custom-db.query", ended[0].Name())

	system, ok := readSpanAttr(ended[0], "db.system.name")
	require.True(t, ok)
	assert.Equal(t, "custom-db", system)

	query, ok := readSpanAttr(ended[0], "db.query.text")
	require.True(t, ok)
	assert.Equal(t, "SELECT $1", query)
	parameter, ok := readSpanAttr(ended[0], "db.query.parameter.1")
	require.True(t, ok)
	assert.Equal(t, "value", parameter)
	require.Len(t, firstHandler.records, 1)
	assert.Empty(t, secondHandler.records)
}

func TestQueryLogNormalizesWhitespaceAndIncludesCallSource(t *testing.T) {
	handler := &testHandler{level: slog.LevelDebug}
	instrumentation := NewInstrumentation("sqlite", &Options{
		Log:      slog.New(handler),
		QueryLog: true,
	})

	ctx, span := instrumentation.StartSpan(t.Context(), "query", "\n\t SELECT  id\nFROM\titems  ")
	EndSpan(span, nil)
	instrumentation.EmitQueryLog(ctx, "query", "\n\t SELECT  id\nFROM\titems  ", time.Millisecond, nil)

	require.Len(t, handler.records, 1)
	query, ok := readRecordAttr(handler.records[0], "db.query.text")
	require.True(t, ok)
	assert.Equal(t, "SELECT id FROM items", query.String())
	file, ok := readRecordAttr(handler.records[0], "code.file.path")
	require.True(t, ok)
	assert.Equal(t, "instrument_test.go", file.String())
	line, ok := readRecordAttr(handler.records[0], "code.line.number")
	require.True(t, ok)
	assert.Positive(t, line.Int64())
}

func TestSlowWarningOmitsQueryTextWhenDebugIsDisabled(t *testing.T) {
	handler := &testHandler{level: slog.LevelInfo}
	instrumentation := NewInstrumentation("sqlite", &Options{
		Log:           slog.New(handler),
		QueryLog:      true,
		SlowThreshold: time.Millisecond,
	})

	instrumentation.EmitQueryLog(t.Context(), "query", "SELECT inline_secret", time.Second, nil)
	require.Len(t, handler.records, 1)
	assert.Equal(t, slog.LevelWarn, handler.records[0].Level)

	hasQueryText := false
	handler.records[0].Attrs(func(attr slog.Attr) bool {
		hasQueryText = hasQueryText || attr.Key == "db.query.text"
		return true
	})
	assert.False(t, hasQueryText)
}

func TestSlowWarningOmitsQueryParametersWhenDebugIsDisabled(t *testing.T) {
	recorder := recordSpans(t)
	handler := &testHandler{level: slog.LevelInfo}
	instrumentation := NewInstrumentation("sqlite", &Options{
		Log:               slog.New(handler),
		QueryLog:          true,
		IncludeParameters: true,
		SlowThreshold:     time.Millisecond,
	})
	parameters := []QueryParameter{{Name: "account_id", Value: "secret-value"}}

	ctx, span := instrumentation.StartQuerySpan(t.Context(), "query", "SELECT  id\nFROM accounts WHERE id = ?", parameters)
	EndSpan(span, nil)
	instrumentation.EmitQueryLogWithParameters(ctx, "query", "SELECT  id\nFROM accounts WHERE id = ?", parameters, time.Second, nil)

	require.Len(t, handler.records, 1)
	_, queryOK := readRecordAttr(handler.records[0], "db.query.text")
	assert.False(t, queryOK)
	_, parameterOK := readRecordAttr(handler.records[0], "db.query.parameter.account_id")
	assert.False(t, parameterOK)

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	spanParameter, ok := readSpanAttr(ended[0], "db.query.parameter.account_id")
	require.True(t, ok)
	assert.Equal(t, "secret-value", spanParameter)
}
