package query

import (
	"context"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

type Q7 struct {
	Query
}

func (q *Q7) Prepare() error {
	if err := kuiper.DefaultEndpoint.DropStream("q7Mem"); err != nil {
		return err
	}
	if err := kuiper.DefaultEndpoint.DropRule("q7Mem"); err != nil {
		return err
	}
	return kuiper.DefaultEndpoint.DropRule("q7")
}

func (q *Q7) SetupQuery() error {
	q7Mem := `{"sql":"create stream q7Mem () WITH ( datasource = \"q7Mem\", FORMAT = \"json\", TYPE= \"memory\")"}`
	if err := kuiper.DefaultEndpoint.CreateStream(q7Mem); err != nil {
		return err
	}
	q7MemQuery := `{
 "id": "q7Mem",
 "sql": "select max(price) as maxprice, window_end() as datetime from bid group by TUMBLINGWINDOW(ss,30)",
 "actions": [
   {
     "memory": {
       "topic": "q7Mem"
     }
   }
 ]
}`
	if err := kuiper.DefaultEndpoint.CreateRawRule(q7MemQuery); err != nil {
		return err
	}
	return kuiper.DefaultEndpoint.CreateRule("q7", "SELECT bid.auction, bid.price, bid.bidder, bid.datetime, bid.extra from bid inner join q7Mem on bid.price = q7Mem.maxprice where bid.datetime < q7Mem.datetime group by TUMBLINGWINDOW(ss,30);")
}

func (q *Q7) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	genrator := nexmark.NewEventGenerator(nexmark.WithExcludePerson(), nexmark.WithExcludeAuction())
	return RunWorkload(ctx, genrator, duration, qps)
}

func (q *Q7) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q7) StartRecvData() {
	startRecvData(&q.Query, "q7")
}

func (q Q7) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q7")
	token.Wait()
	if err := kuiper.DefaultEndpoint.DropStream("q7Mem"); err != nil {
		return err
	}
	if err := kuiper.DefaultEndpoint.DropRule("q7Mem"); err != nil {
		return err
	}
	return kuiper.DefaultEndpoint.DropRule("q7")
}
