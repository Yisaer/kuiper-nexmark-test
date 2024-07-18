package query

import (
	"context"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

type Q3 struct {
	Query
}

func (q *Q3) Prepare() error {
	return kuiper.DefaultEndpoint.DropRule("q3")
}

//SELECT person.name, person.city, person.state, auction.id FROM auction INNER JOIN person on auction.seller = person.id WHERE auction.category = 10 and (person.state = 'OR' OR person.state = 'ID' OR person.state = 'CA');

func (q *Q3) SetupQuery() error {
	return kuiper.DefaultEndpoint.CreateRule("q3", "SELECT person.name, person.city, person.state, auction.id FROM auction INNER JOIN person on auction.seller = person.id WHERE auction.category = 3 and (person.state = 'or' OR person.state = 'id' OR person.state = 'ca') group by TUMBLINGWINDOW(ss, 30);")
}

func (q *Q3) Clear() error {
	token := DefaultMQTTClient.Unsubscribe("q3")
	token.Wait()
	return kuiper.DefaultEndpoint.DropRule("q3")
}

func (q *Q3) GetResult() (int, int) {
	return q.TotalRecordsOut, q.TotalRecordsProcessed
}

func (q *Q3) RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus {
	generator := nexmark.NewEventGenerator(nexmark.WithExcludeBid())
	return RunWorkload(ctx, generator, duration, qps)
}

func (q *Q3) StartRecvData() {
	startRecvData(&q.Query, "q3")
}
