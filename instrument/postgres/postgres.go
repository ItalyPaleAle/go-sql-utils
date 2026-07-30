// Package postgres provides a pgx tracer that emits OpenTelemetry spans and optional query logs for PostgreSQL connection pools
package postgres

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/go-sql-utils/instrument"
)

type pgxTracer struct {
	instrumentation *instrument.Instrumentation
	chain           []pgx.QueryTracer
}

// ctxKey includes the tracer instance so nested pgxTracer values cannot overwrite one another's call state
type ctxKey struct {
	tracer *pgxTracer
}

type pgxCallState struct {
	span      trace.Span
	start     time.Time
	lastQuery time.Time
	statement string
	firstErr  error
}

type logKind uint8

const (
	logNone logKind = iota
	logQuery
	logSlowOperation
)

// NewTracer returns a pgx tracer for queries, batches, COPY, prepares, connects, and pool acquisition
// opts may be nil
// Chained tracers continue to receive every interface they implement
func NewTracer(opts *instrument.Options, chain ...pgx.QueryTracer) pgx.QueryTracer {
	// The tracer is shared by the pool for its lifetime, so own the slice rather than retaining or mutating caller storage
	chain = slices.DeleteFunc(slices.Clone(chain), func(t pgx.QueryTracer) bool {
		return t == nil
	})

	return &pgxTracer{
		instrumentation: instrument.NewInstrumentation("postgresql", opts),
		chain:           chain,
	}
}

//nolint:spancheck // The span is ended by the matching pgx End callback
func (t *pgxTracer) startCall(ctx context.Context, op, statement string) context.Context {
	spanCtx, span := t.instrumentation.StartSpan(ctx, op, statement)
	now := time.Now()
	state := &pgxCallState{
		span:      span,
		start:     now,
		lastQuery: now,
		statement: statement,
	}
	return context.WithValue(spanCtx, ctxKey{tracer: t}, state)
}

func (t *pgxTracer) callState(ctx context.Context) *pgxCallState {
	state, _ := ctx.Value(ctxKey{tracer: t}).(*pgxCallState)
	return state
}

func (t *pgxTracer) endCall(ctx context.Context, op string, err error, logging logKind) {
	state := t.callState(ctx)
	if state == nil {
		return
	}

	err = errors.Join(state.firstErr, err)
	dur := time.Since(state.start)
	instrument.EndSpan(state.span, err)

	switch logging {
	case logQuery:
		t.instrumentation.EmitQueryLog(ctx, op, state.statement, dur, err)
	case logSlowOperation:
		t.instrumentation.EmitSlowOperationLog(ctx, op, dur, err)
	case logNone:
	}
}

func (t *pgxTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = t.startCall(ctx, "query", data.SQL)
	for _, chained := range t.chain {
		//nolint:fatcontext // Each chained tracer must receive the context returned by the previous tracer
		ctx = chained.TraceQueryStart(ctx, conn, data)
	}
	return ctx
}

func (t *pgxTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	for _, v := range slices.Backward(t.chain) {
		v.TraceQueryEnd(ctx, conn, data)
	}
	t.endCall(ctx, "query", data.Err, logQuery)
}

func (t *pgxTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	ctx = t.startCall(ctx, "batch", "")
	for _, chained := range t.chain {
		tracer, ok := chained.(pgx.BatchTracer)
		if ok {
			//nolint:fatcontext // Each chained tracer must receive the context returned by the previous tracer
			ctx = tracer.TraceBatchStart(ctx, conn, data)
		}
	}
	return ctx
}

func (t *pgxTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	state := t.callState(ctx)
	if state != nil {
		now := time.Now()
		t.instrumentation.AddQueryEvent(ctx, state.span, data.SQL)
		t.instrumentation.EmitQueryLog(ctx, "query", data.SQL, now.Sub(state.lastQuery), data.Err)
		state.lastQuery = now
		if data.Err != nil && state.firstErr == nil {
			state.firstErr = data.Err
		}
	}

	for _, chained := range t.chain {
		tracer, ok := chained.(pgx.BatchTracer)
		if ok {
			tracer.TraceBatchQuery(ctx, conn, data)
		}
	}
}

func (t *pgxTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	for _, v := range slices.Backward(t.chain) {
		tracer, ok := v.(pgx.BatchTracer)
		if ok {
			tracer.TraceBatchEnd(ctx, conn, data)
		}
	}

	t.endCall(ctx, "batch", data.Err, logSlowOperation)
}

func (t *pgxTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	ctx = t.startCall(ctx, "copy_from", "")
	for _, chained := range t.chain {
		tracer, ok := chained.(pgx.CopyFromTracer)
		if ok {
			//nolint:fatcontext // Each chained tracer must receive the context returned by the previous tracer
			ctx = tracer.TraceCopyFromStart(ctx, conn, data)
		}
	}

	return ctx
}

func (t *pgxTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	for _, v := range slices.Backward(t.chain) {
		tracer, ok := v.(pgx.CopyFromTracer)
		if ok {
			tracer.TraceCopyFromEnd(ctx, conn, data)
		}
	}

	t.endCall(ctx, "copy_from", data.Err, logSlowOperation)
}

func (t *pgxTracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	ctx = t.startCall(ctx, "prepare", data.SQL)
	for _, chained := range t.chain {
		tracer, ok := chained.(pgx.PrepareTracer)
		if ok {
			//nolint:fatcontext // Each chained tracer must receive the context returned by the previous tracer
			ctx = tracer.TracePrepareStart(ctx, conn, data)
		}
	}

	return ctx
}

func (t *pgxTracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	for _, v := range slices.Backward(t.chain) {
		tracer, ok := v.(pgx.PrepareTracer)
		if ok {
			tracer.TracePrepareEnd(ctx, conn, data)
		}
	}

	t.endCall(ctx, "prepare", data.Err, logNone)
}

func (t *pgxTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	ctx = t.startCall(ctx, "connect", "")
	for _, chained := range t.chain {
		tracer, ok := chained.(pgx.ConnectTracer)
		if ok {
			//nolint:fatcontext // Each chained tracer must receive the context returned by the previous tracer
			ctx = tracer.TraceConnectStart(ctx, data)
		}
	}

	return ctx
}

func (t *pgxTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	for _, v := range slices.Backward(t.chain) {
		tracer, ok := v.(pgx.ConnectTracer)
		if ok {
			tracer.TraceConnectEnd(ctx, data)
		}
	}

	t.endCall(ctx, "connect", data.Err, logSlowOperation)
}

func (t *pgxTracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	ctx = t.startCall(ctx, "pool.acquire", "")
	for _, chained := range t.chain {
		tracer, ok := chained.(pgxpool.AcquireTracer)
		if ok {
			//nolint:fatcontext // Each chained tracer must receive the context returned by the previous tracer
			ctx = tracer.TraceAcquireStart(ctx, pool, data)
		}
	}

	return ctx
}

func (t *pgxTracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	for _, v := range slices.Backward(t.chain) {
		tracer, ok := v.(pgxpool.AcquireTracer)
		if ok {
			tracer.TraceAcquireEnd(ctx, pool, data)
		}
	}

	t.endCall(ctx, "pool.acquire", data.Err, logSlowOperation)
}

func (t *pgxTracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	for _, v := range slices.Backward(t.chain) {
		tracer, ok := v.(pgxpool.ReleaseTracer)
		if ok {
			tracer.TraceRelease(pool, data)
		}
	}
}
