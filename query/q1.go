package query

import (
	"context"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

type Q1 struct {
	Query
}

func (q *Q1) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q1")
}

func (q *Q1) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q1", "SELECT auction, 0.9 * price, bidder, datetime FROM bid;")
}

func (q *Q1) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q1")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q1")
}

func (q *Q1) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q1) StartRecvData() {
	startRecvData(&q.Query, "q1")
}

func (q *Q1) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	generator := nexmark.NewEventGenerator(nexmark.WithExcludePerson(), nexmark.WithExcludeAuction())
	return RunWorkload(ctx, generator, duration, qps)
}
