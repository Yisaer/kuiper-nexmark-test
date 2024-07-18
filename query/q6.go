package query

import (
	"context"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

var q6SQL = "Select row_number() over (partition by auction.id, auction.seller order by bid.price desc),  auction.id, auction.seller,bid.price,bid.datetime from auction inner join bid on auction.id = bid.auction where bid.datetime >= auction.datetime and bid.datetime <= auction.expires group by tumblingWindow(ss,30)"

type Q6 struct {
	Query
}

func (q *Q6) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q6")
}

func (q *Q6) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q6", q6SQL)
}

func (q *Q6) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	genrator := nexmark.NewEventGenerator(nexmark.WithExcludePerson())
	return RunWorkload(ctx, genrator, duration, qps)
}

func (q *Q6) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q6) StartRecvData() {
	startRecvData(&q.Query, "q6")
}

func (q *Q6) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q4")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q6")
}
