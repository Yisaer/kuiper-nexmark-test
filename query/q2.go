package query

import (
	"context"
	"time"

	"nexmark-go/kuiper"
	"nexmark-go/nexmark"
)

type Q2 struct {
	Query
}

func (q *Q2) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q2")
}

func (q *Q2) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q2", "SELECT auction, price FROM bid WHERE MOD(auction, 3) > 0;")
}

func (q *Q2) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q2")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q2")
}

func (q *Q2) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q2 *Q2) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	generator := nexmark.NewEventGenerator(nexmark.WithExcludePerson(), nexmark.WithExcludeAuction())
	return RunWorkload(ctx, generator, duration, qps)
}

func (q *Q2) StartRecvData() {
	startRecvData(&q.Query, "q2")
}
