package query

import (
	"context"
	"time"

	"nexmark-go/kuiper"
	"nexmark-go/nexmark"
)

var q9sql = "select bid.auction,bid.bidder,bid.price, bid.datetime, bid.extra, row_number() over (partition by auction.id order by bid.price desc, bid.datetime asc) as rownum from auction inner join bid on auction.id = bid.auction and bid.datetime >= auction.datetime and bid.datetime <= auction.expires group by tumblingWindow(ss,30)"

type Q9 struct {
	Query
}

func (q *Q9) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q9")
}

func (q *Q9) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q9", q9sql)
}

func (q *Q9) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	genrator := nexmark.NewEventGenerator(nexmark.WithExcludePerson())
	return RunWorkload(ctx, genrator, duration, qps)
}

func (q *Q9) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q9) StartRecvData() {
	startRecvData(&q.Query, "q9")
}

func (q *Q9) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q9")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q9")
}
