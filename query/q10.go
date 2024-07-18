package query

import (
	"context"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

var q10sql = `SELECT auction, bidder, price, datetime, extra, format_time(datetime, 'yyyy-MM-dd') as f1, format_time(datetime, 'HH:mm') as f2 FROM bid;`

type Q10 struct {
	Query
}

func (q *Q10) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q10")
}

func (q *Q10) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q10", q10sql)
}

func (q *Q10) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	genrator := nexmark.NewEventGenerator(nexmark.WithExcludePerson(), nexmark.WithExcludeAuction())
	return RunWorkload(ctx, genrator, duration, qps)
}

func (q *Q10) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q10) StartRecvData() {
	startRecvData(&q.Query, "q10")
}

func (q *Q10) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q10")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q10")
}
