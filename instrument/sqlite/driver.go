package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/go-sql-utils/instrument"
)

// These interfaces deliberately describe modernc.org/sqlite (as of v1.55) driver surface
type sqliteConn interface {
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
	driver.Validator
}

type sqliteStmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

type sqliteRows interface {
	driver.Rows
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeLength
	driver.RowsColumnTypeNullable
	driver.RowsColumnTypePrecisionScale
	driver.RowsColumnTypeScanType
}

type connector struct {
	base            driver.Connector
	instrumentation *instrument.Instrumentation
}

func newConnector(base driver.Connector, instrumentation *instrument.Instrumentation) *connector {
	return &connector{base: base, instrumentation: instrumentation}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}

	base, ok := conn.(sqliteConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("unsupported SQLite driver connection %T", conn)
	}

	return &wrappedConn{base: base, instrumentation: c.instrumentation}, nil
}

func (c *connector) Driver() driver.Driver {
	return c.base.Driver()
}

type wrappedConn struct {
	base            sqliteConn
	instrumentation *instrument.Instrumentation
}

func (c *wrappedConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *wrappedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	_, span := c.instrumentation.StartSpan(ctx, "prepare", query)

	stmt, err := c.base.PrepareContext(ctx, query)
	if err != nil {
		instrument.EndSpan(span, err)
		return nil, err
	}

	base, ok := stmt.(sqliteStmt)
	if !ok {
		_ = stmt.Close()
		err = fmt.Errorf("unsupported SQLite driver statement %T", stmt)
		instrument.EndSpan(span, err)
		return nil, err
	}

	instrument.EndSpan(span, nil)
	return &wrappedStmt{base: base, statement: query, instrumentation: c.instrumentation}, nil
}

func (c *wrappedConn) Close() error {
	return c.base.Close()
}

func (c *wrappedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	_, span := c.instrumentation.StartSpan(ctx, "transaction", "")

	tx, err := c.base.BeginTx(ctx, opts)
	if err != nil {
		instrument.EndSpan(span, err)
		return nil, err
	}

	return &wrappedTx{base: tx, span: span}, nil
}

func (c *wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	parameters := namedQueryParameters(args)
	spanCtx, span := c.instrumentation.StartQuerySpan(ctx, "exec", query, parameters)
	start := time.Now()

	res, err := c.base.ExecContext(ctx, query, args)
	instrument.EndSpan(span, err)
	c.instrumentation.EmitQueryLogWithParameters(spanCtx, "exec", query, parameters, time.Since(start), err)
	return res, err
}

func (c *wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	parameters := namedQueryParameters(args)
	spanCtx, span := c.instrumentation.StartQuerySpan(ctx, "query", query, parameters)
	start := time.Now()

	rows, err := c.base.QueryContext(ctx, query, args)
	if err != nil {
		instrument.EndSpan(span, err)
		c.instrumentation.EmitQueryLogWithParameters(spanCtx, "query", query, parameters, time.Since(start), err)
		return nil, err
	}

	base, ok := rows.(sqliteRows)
	if !ok {
		_ = rows.Close()
		err = fmt.Errorf("unsupported SQLite driver rows %T", rows)
		instrument.EndSpan(span, err)
		c.instrumentation.EmitQueryLogWithParameters(spanCtx, "query", query, parameters, time.Since(start), err)
		return nil, err
	}

	return newWrappedRows(base, span, func(rowErr error) {
		c.instrumentation.EmitQueryLogWithParameters(spanCtx, "query", query, parameters, time.Since(start), rowErr)
	}), nil
}

func (c *wrappedConn) Ping(ctx context.Context) error {
	return c.base.Ping(ctx)
}

func (c *wrappedConn) ResetSession(ctx context.Context) error {
	return c.base.ResetSession(ctx)
}

func (c *wrappedConn) IsValid() bool {
	return c.base.IsValid()
}

type wrappedStmt struct {
	base            sqliteStmt
	statement       string
	instrumentation *instrument.Instrumentation
}

func (s *wrappedStmt) Close() error {
	return s.base.Close()
}

func (s *wrappedStmt) NumInput() int {
	return s.base.NumInput()
}

func (s *wrappedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.exec(context.Background(), valueQueryParameters(args), func() (driver.Result, error) {
		return s.base.Exec(args)
	})
}

func (s *wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.exec(ctx, namedQueryParameters(args), func() (driver.Result, error) {
		return s.base.ExecContext(ctx, args)
	})
}

func (s *wrappedStmt) exec(ctx context.Context, parameters []instrument.QueryParameter, exec func() (driver.Result, error)) (driver.Result, error) {
	spanCtx, span := s.instrumentation.StartQuerySpan(ctx, "exec", s.statement, parameters)
	start := time.Now()

	res, err := exec()
	instrument.EndSpan(span, err)
	s.instrumentation.EmitQueryLogWithParameters(spanCtx, "exec", s.statement, parameters, time.Since(start), err)
	return res, err
}

func (s *wrappedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.queryRows(context.Background(), valueQueryParameters(args), func() (driver.Rows, error) {
		return s.base.Query(args)
	})
}

func (s *wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.queryRows(ctx, namedQueryParameters(args), func() (driver.Rows, error) {
		return s.base.QueryContext(ctx, args)
	})
}

func (s *wrappedStmt) queryRows(ctx context.Context, parameters []instrument.QueryParameter, query func() (driver.Rows, error)) (driver.Rows, error) {
	spanCtx, span := s.instrumentation.StartQuerySpan(ctx, "query", s.statement, parameters)
	start := time.Now()

	rows, err := query()
	if err != nil {
		instrument.EndSpan(span, err)
		s.instrumentation.EmitQueryLogWithParameters(spanCtx, "query", s.statement, parameters, time.Since(start), err)
		return nil, err
	}

	base, ok := rows.(sqliteRows)
	if !ok {
		_ = rows.Close()
		err = fmt.Errorf("unsupported SQLite driver rows %T", rows)
		instrument.EndSpan(span, err)
		s.instrumentation.EmitQueryLogWithParameters(spanCtx, "query", s.statement, parameters, time.Since(start), err)
		return nil, err
	}

	return newWrappedRows(base, span, func(rowErr error) {
		s.instrumentation.EmitQueryLogWithParameters(spanCtx, "query", s.statement, parameters, time.Since(start), rowErr)
	}), nil
}

type wrappedTx struct {
	base driver.Tx
	span trace.Span
	done sync.Once
}

func (t *wrappedTx) Commit() error {
	err := t.base.Commit()
	t.done.Do(func() {
		instrument.EndSpan(t.span, err)
	})
	return err
}

func (t *wrappedTx) Rollback() error {
	err := t.base.Rollback()
	t.done.Do(func() {
		instrument.EndSpan(t.span, err)
	})
	return err
}

type wrappedRows struct {
	base     sqliteRows
	span     trace.Span
	logQuery func(error)

	mu           sync.Mutex
	iterationErr error
	endOnce      sync.Once
}

func newWrappedRows(base sqliteRows, span trace.Span, logQuery func(error)) *wrappedRows {
	return &wrappedRows{base: base, span: span, logQuery: logQuery}
}

func (r *wrappedRows) Columns() []string {
	return r.base.Columns()
}

func (r *wrappedRows) Close() error {
	closeErr := r.base.Close()
	r.endOnce.Do(func() {
		rErr := errors.Join(r.getIterationErr(), closeErr)
		instrument.EndSpan(r.span, rErr)
		r.logQuery(rErr)
	})

	return closeErr
}

func (r *wrappedRows) Next(dest []driver.Value) error {
	err := r.base.Next(dest)
	if err != nil && !errors.Is(err, io.EOF) {
		r.mu.Lock()
		if r.iterationErr == nil {
			r.iterationErr = err
		}
		r.mu.Unlock()
	}

	return err
}

func (r *wrappedRows) getIterationErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.iterationErr
}

func (r *wrappedRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.base.ColumnTypeDatabaseTypeName(index)
}

func (r *wrappedRows) ColumnTypeLength(index int) (int64, bool) {
	return r.base.ColumnTypeLength(index)
}

func (r *wrappedRows) ColumnTypeNullable(index int) (bool, bool) {
	return r.base.ColumnTypeNullable(index)
}

func (r *wrappedRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	return r.base.ColumnTypePrecisionScale(index)
}

func (r *wrappedRows) ColumnTypeScanType(index int) reflect.Type {
	return r.base.ColumnTypeScanType(index)
}

func namedQueryParameters(args []driver.NamedValue) []instrument.QueryParameter {
	parameters := make([]instrument.QueryParameter, len(args))
	for i, arg := range args {
		name := arg.Name
		if name == "" {
			name = strconv.Itoa(arg.Ordinal)
		}
		parameters[i] = instrument.QueryParameter{
			Name:  name,
			Value: arg.Value,
		}
	}

	return parameters
}

func valueQueryParameters(args []driver.Value) []instrument.QueryParameter {
	parameters := make([]instrument.QueryParameter, len(args))
	for i, arg := range args {
		parameters[i] = instrument.QueryParameter{
			Name:  strconv.Itoa(i + 1),
			Value: arg,
		}
	}

	return parameters
}

var (
	_ driver.Connector          = (*connector)(nil)
	_ driver.Conn               = (*wrappedConn)(nil)
	_ driver.ConnPrepareContext = (*wrappedConn)(nil)
	_ driver.ConnBeginTx        = (*wrappedConn)(nil)
	_ driver.ExecerContext      = (*wrappedConn)(nil)
	_ driver.QueryerContext     = (*wrappedConn)(nil)
	_ driver.Pinger             = (*wrappedConn)(nil)
	_ driver.SessionResetter    = (*wrappedConn)(nil)
	_ driver.Validator          = (*wrappedConn)(nil)
	_ driver.Stmt               = (*wrappedStmt)(nil)
	_ driver.StmtExecContext    = (*wrappedStmt)(nil)
	_ driver.StmtQueryContext   = (*wrappedStmt)(nil)
	_ driver.Rows               = (*wrappedRows)(nil)
	_ driver.Tx                 = (*wrappedTx)(nil)
)
