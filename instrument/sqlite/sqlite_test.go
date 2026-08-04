package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/italypaleale/go-sql-utils/instrument"
	sqliteutils "github.com/italypaleale/go-sql-utils/sqlite"
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

// openTestDB opens an instrumented in-memory SQLite database through the same prepared connector used in production
func openTestDB(t *testing.T, opts *instrument.Options) *sql.DB {
	t.Helper()

	connector, err := sqliteutils.NewConnector(sqliteutils.ConnectOpts{ConnString: ":memory:"})
	require.NoError(t, err)

	db, err := Open(connector, opts)
	require.NoError(t, err)
	assert.Equal(t, 1, db.Stats().MaxOpenConnections)

	t.Cleanup(func() {
		closeErr := db.Close()
		assert.NoError(t, closeErr)
	})

	return db
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

func TestExecAndQueryProduceSpans(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, &instrument.Options{
		Log:      slog.New(newCaptureHandler()),
		QueryLog: true,
	})
	ctx := t.Context()

	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO items (id, val) VALUES (?, ?)", "a1", "secret-value-123")
	require.NoError(t, err)

	var got string
	err = db.QueryRowContext(ctx, "SELECT val FROM items WHERE id = ?", "a1").Scan(&got)
	require.NoError(t, err)
	assert.Equal(t, "secret-value-123", got)

	byName := spansByName(t, sr)

	// Two execs and one query must have been spanned
	execs := make([]sdktrace.ReadOnlySpan, 0)
	for _, s := range sr.Ended() {
		if s.Name() == "sqlite.exec" {
			execs = append(execs, s)
		}
	}
	require.Len(t, execs, 2)

	query, ok := byName["sqlite.query"]
	require.True(t, ok, "expected a sqlite.query span")

	// Spans carry the statement text but never the parameter values
	for _, s := range append(execs, query) {
		_, ok = spanAttr(s, "db.query.text")
		assert.True(t, ok, "span %q missing db.query.text", s.Name())

		system, systemOK := spanAttr(s, "db.system.name")
		assert.True(t, systemOK)
		assert.Equal(t, "sqlite", system)

		operation, operationOK := spanAttr(s, "db.operation.name")
		assert.True(t, operationOK)
		assert.NotEmpty(t, operation)

		for _, attr := range s.Attributes() {
			assert.NotContains(t, attr.Value.AsString(), "secret-value-123", "span attributes must not contain parameter values")
		}
	}
}

func TestQuerySpanEndsAfterRowsConsumed(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, nil)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO items (id) VALUES ('a'), ('b'), ('c')")
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "SELECT id FROM items")
	require.NoError(t, err)
	// Close is called explicitly below to assert the span ends at that point
	// The deferred call is only a safety net
	defer func() {
		_ = rows.Close()
	}()

	// While the rows are open, the query span must not have ended yet
	assert.Empty(t, spansByName(t, sr)["sqlite.query"], "query span should still be open while rows are unconsumed")

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	assert.Equal(t, 3, count)

	// Closing the rows ends exactly one query span
	querySpans := 0
	for _, s := range sr.Ended() {
		if s.Name() == "sqlite.query" {
			querySpans++
		}
	}
	assert.Equal(t, 1, querySpans)
}

func TestStatementErrorIsRecordedOnSpan(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, nil)

	_, err := db.ExecContext(t.Context(), "INSERT INTO no_such_table VALUES (1)")
	require.Error(t, err)

	span := spansByName(t, sr)["sqlite.exec"]
	require.NotNil(t, span, "expected a sqlite.exec span")
	assert.Equal(t, codes.Error, span.Status().Code)
}

func TestTransactionSpans(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, nil)
	ctx := t.Context()

	// A committed transaction produces a tx span that stays open for the transaction's lifetime
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	assert.Empty(t, spansByName(t, sr)["sqlite.transaction"], "transaction span should still be open before commit")
	require.NoError(t, tx.Commit())

	// A rolled-back transaction produces a tx span too
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	txSpans := 0
	for _, s := range sr.Ended() {
		if s.Name() == "sqlite.transaction" {
			txSpans++
			assert.NotEqual(t, codes.Error, s.Status().Code)
		}
	}
	assert.Equal(t, 2, txSpans)
}

func TestQueryLogEmitsDebugRecordsWithoutParameters(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	log := slog.New(handler)
	db := openTestDB(t, &instrument.Options{
		Log:      log,
		QueryLog: true,
	})
	ctx := t.Context()

	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO items (id, val) VALUES (?, ?)", "param-id-xyz", "secret-value-123")
	require.NoError(t, err)

	// Two Debug records with the statement text and no parameter values
	require.Len(t, handler.records, 2)
	for _, r := range handler.records {
		assert.Equal(t, slog.LevelDebug, r.Level)
		assert.Equal(t, "Executed SQL statement", r.Message)
		for _, v := range attrStrings(r) {
			assert.NotContains(t, v, "secret-value-123")
			assert.NotContains(t, v, "param-id-xyz")
		}
	}

	statements := make([]string, 0, len(handler.records))
	for _, r := range handler.records {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "db.query.text" {
				statements = append(statements, a.Value.String())
			}
			return true
		})
	}
	assert.Contains(t, statements, "INSERT INTO items (id, val) VALUES (?, ?)")
}

func TestQueryParametersAndNormalizedTextAreIncludedWhenEnabled(t *testing.T) {
	sr := setupSpanRecorder(t)
	handler := newCaptureHandler()
	db := openTestDB(t, &instrument.Options{
		Log:               slog.New(handler),
		QueryLog:          true,
		IncludeParameters: true,
	})
	ctx := t.Context()

	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "\n\tINSERT  INTO items (id, val)\nVALUES (:item_id, ?)\t", sql.Named("item_id", "param-id-xyz"), "secret-value-123")
	require.NoError(t, err)

	span := spansByName(t, sr)["sqlite.exec"]
	require.NotNil(t, span)
	itemID, ok := spanAttr(span, "db.query.parameter.item_id")
	require.True(t, ok)
	assert.Equal(t, "param-id-xyz", itemID)
	value, ok := spanAttr(span, "db.query.parameter.2")
	require.True(t, ok)
	assert.Equal(t, "secret-value-123", value)

	require.Len(t, handler.records, 2)
	record := handler.records[1]
	query, ok := recordAttr(record, "db.query.text")
	require.True(t, ok)
	assert.Equal(t, "INSERT INTO items (id, val) VALUES (:item_id, ?)", query.String())
	logItemID, ok := recordAttr(record, "db.query.parameter.item_id")
	require.True(t, ok)
	assert.Equal(t, "param-id-xyz", logItemID.String())
	logValue, ok := recordAttr(record, "db.query.parameter.2")
	require.True(t, ok)
	assert.Equal(t, "secret-value-123", logValue.String())
	file, ok := recordAttr(record, "code.file.path")
	require.True(t, ok)
	assert.Equal(t, "sqlite_test.go", file.String())
	line, ok := recordAttr(record, "code.line.number")
	require.True(t, ok)
	assert.Positive(t, line.Int64())
}

func TestSlowThresholdEmitsWarnRecords(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	db := openTestDB(t, &instrument.Options{
		Log:           slog.New(handler),
		SlowThreshold: time.Nanosecond,
	})

	_, err := db.ExecContext(t.Context(), "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	// Every statement is slower than a nanosecond, so all of them are logged at Warn
	require.NotEmpty(t, handler.records)
	for _, r := range handler.records {
		assert.Equal(t, slog.LevelWarn, r.Level)
		assert.Equal(t, "Slow SQL statement", r.Message)
	}
}

func TestSlowThresholdSkipsFastStatements(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()
	db := openTestDB(t, &instrument.Options{
		Log:           slog.New(handler),
		SlowThreshold: time.Hour,
	})

	_, err := db.ExecContext(t.Context(), "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	assert.Empty(t, handler.records)
}

func TestColumnTypesArePreserved(t *testing.T) {
	setupSpanRecorder(t)
	db := openTestDB(t, nil)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY, n INTEGER)")
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "SELECT id, n FROM items")
	require.NoError(t, err)
	defer func() {
		_ = rows.Close()
	}()

	types, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, types, 2)

	// The wrapped rows must still expose the driver's column metadata
	assert.Equal(t, "TEXT", types[0].DatabaseTypeName())
	assert.Equal(t, "INTEGER", types[1].DatabaseTypeName())
}

func TestPrepareAndStmtAreInstrumented(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, &instrument.Options{
		Log:      slog.New(newCaptureHandler()),
		QueryLog: true,
	})
	ctx := t.Context()

	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	stmt, err := db.PrepareContext(ctx, "INSERT INTO items (id) VALUES (?)")
	require.NoError(t, err)
	defer func() {
		_ = stmt.Close()
	}()

	_, err = stmt.ExecContext(ctx, "a1")
	require.NoError(t, err)

	byName := spansByName(t, sr)
	assert.NotNil(t, byName["sqlite.prepare"], "expected a sqlite.prepare span")

	var stmtExecFound bool
	for _, s := range sr.Ended() {
		if s.Name() == "sqlite.exec" {
			stmt, ok := spanAttr(s, "db.query.text")
			if ok && stmt == "INSERT INTO items (id) VALUES (?)" {
				stmtExecFound = true
			}
		}
	}
	assert.True(t, stmtExecFound, "expected a sqlite.exec span for the prepared statement")
}

func TestErrorStatusNotSetOnSuccess(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, nil)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	for _, s := range sr.Ended() {
		assert.NotEqual(t, codes.Error, s.Status().Code, "span %q should not be failed", s.Name())
	}
}

// TestSpansAreChildrenOfCaller verifies the statement spans hang off the span in the caller's context
func TestSpansAreChildrenOfCaller(t *testing.T) {
	sr := setupSpanRecorder(t)
	db := openTestDB(t, nil)

	ctx, parent := otel.Tracer("test").Start(t.Context(), "parent")
	_, err := db.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY)")
	require.NoError(t, err)
	parent.End()

	span := spansByName(t, sr)["sqlite.exec"]
	require.NotNil(t, span)
	assert.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
}

func TestInMemoryPoolIsSafeUnderConcurrency(t *testing.T) {
	setupSpanRecorder(t)
	db := openTestDB(t, nil)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE items (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)

	const workers = 24
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			_, execErr := db.ExecContext(t.Context(), "INSERT INTO items (id) VALUES (?)", i)
			errCh <- execErr
		})
	}
	wg.Wait()

	close(errCh)

	for execErr := range errCh {
		require.NoError(t, execErr)
	}

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM items").Scan(&count))
	assert.Equal(t, workers, count)
}

var errRowsIteration = errors.New("rows iteration failed")

type failingRows struct{}

func (failingRows) Columns() []string {
	return []string{"value"}
}

func (failingRows) Close() error {
	return nil
}

func (failingRows) Next([]driver.Value) error {
	return errRowsIteration
}

func (failingRows) ColumnTypeDatabaseTypeName(int) string {
	return "TEXT"
}

func (failingRows) ColumnTypeLength(int) (int64, bool) {
	return 0, false
}

func (failingRows) ColumnTypeNullable(int) (bool, bool) {
	return true, true
}

func (failingRows) ColumnTypePrecisionScale(int) (int64, int64, bool) {
	return 0, 0, false
}

func (failingRows) ColumnTypeScanType(int) reflect.Type {
	return reflect.TypeFor[string]()
}

func TestRowsIterationErrorFailsQuerySpanAndLog(t *testing.T) {
	sr := setupSpanRecorder(t)
	instrumentation := instrument.NewInstrumentation("sqlite", nil)
	_, span := instrumentation.StartSpan(t.Context(), "query", "SELECT value")

	var loggedErr error
	rows := newWrappedRows(failingRows{}, span, func(err error) {
		loggedErr = err
	})

	err := rows.Next(make([]driver.Value, 1))
	require.ErrorIs(t, err, errRowsIteration)
	err = rows.Close()
	require.NoError(t, err)

	ended := spansByName(t, sr)["sqlite.query"]
	require.NotNil(t, ended)
	assert.Equal(t, codes.Error, ended.Status().Code)
	assert.ErrorIs(t, loggedErr, errRowsIteration)
}
