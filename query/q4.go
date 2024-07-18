package query

import (
	"context"
	"fmt"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

type Q4 struct {
	Query
}

func (q *Q4) StartRecvData() {
	startRecvData(&q.Query, "q4")
}

func (q *Q4) Prepare() error {
	if err := kuiper.DefaultEndpoint.DropStream("q4Mem"); err != nil {
		return err
	}
	if err := kuiper.DefaultEndpoint.DropRule("q4Mem"); err != nil {
		return err
	}
	return kuiper.DefaultEndpoint.DropRule("q4")
}

func (q *Q4) SetupQuery() error {
	q4Mem := `{"sql":"create stream q4Mem () WITH ( datasource = \"q4Mem\", FORMAT = \"json\", TYPE= \"memory\")"}`
	if err := kuiper.DefaultEndpoint.CreateStream(q4Mem); err != nil {
		return err
	}
	q4MemQuery := fmt.Sprintf(`{
 "id": "q4Mem",
 "sql": "SELECT MAX(bid.price) AS final, auction.category FROM auction inner join bid WHERE auction.id = bid.auction AND bid.datetime BETWEEN auction.datetime AND auction.expires group by TUMBLINGWINDOW(ss, 30),auction.id, auction.category",
 "actions": [
   {
     "memory": {
       "topic": "q4Mem"
     }
   }
 ],
  "options": {
        "bufferLength":%v
    }
}`, BufferLength)
	if err := kuiper.DefaultEndpoint.CreateRawRule(q4MemQuery); err != nil {
		return err
	}
	return kuiper.DefaultEndpoint.CreateRule("q4", "SELECT q4Mem.category, AVG(q4Mem.final) FROM q4Mem GROUP BY TUMBLINGWINDOW(ss, 30), q4Mem.category;")
}

func (q *Q4) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q4")
	token.Wait()
	if err := kuiper.DefaultEndpoint.DropStream("q4Mem"); err != nil {
		return err
	}
	if err := kuiper.DefaultEndpoint.DropRule("q4Mem"); err != nil {
		return err
	}
	return kuiper.DefaultEndpoint.DropRule("q4")
}

func (q *Q4) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q4) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	generator := nexmark.NewEventGenerator(nexmark.WithExcludePerson())
	return RunWorkload(ctx, generator, duration, qps)
}
