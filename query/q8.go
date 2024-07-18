package query

import (
	"context"
	"time"

	"nexmark-go/kuiper"
	"nexmark-go/nexmark"
)

type Q8 struct {
	Query
}

func (q *Q8) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q8")
}

func (q *Q8) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q8", "select person.id,person.name,window_start() from person inner join auction on person.id = auction.seller group by TUMBLINGWINDOW(ss,30)")
}

func (q *Q8) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	genrator := nexmark.NewEventGenerator(nexmark.WithExcludeBid())
	return RunWorkload(ctx, genrator, duration, qps)
}

func (q *Q8) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q8) StartRecvData() {
	startRecvData(&q.Query, "q8")
}

func (q *Q8) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q8")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q8")
}
