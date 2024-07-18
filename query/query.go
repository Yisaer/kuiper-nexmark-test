package query

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/time/rate"

	"github.com/yisaer/kuiper-nexmark-test/nexmark"
)

var BufferLength int

var Quries map[string]QueryCase

func init() {
	Quries = make(map[string]QueryCase)
	Quries["q1"] = &Q1{}
	Quries["q2"] = &Q2{}
	Quries["q3"] = &Q3{}
	Quries["q4"] = &Q4{}
	Quries["q6"] = &Q6{}
	Quries["q7"] = &Q7{}
	Quries["q8"] = &Q8{}
	Quries["q9"] = &Q9{}
	Quries["q10"] = &Q10{}
}

type QueryCase interface {
	Prepare() error
	SetupQuery() error
	RunWorkload(ctx context.Context, duration time.Duration, qps int) RecordsSendStatus
	GetResult() (int, int)
	StartRecvData()
	Clear() error
}

func RunWorkload(ctx context.Context, generator *nexmark.EventGenerator, duration time.Duration, qps int) RecordsSendStatus {
	speed := time.Second / time.Duration(qps)
	limiter := rate.NewLimiter(rate.Every(speed), qps)
	after := time.After(duration)
	status := RecordsSendStatus{}
	result := generator.Gen(9999)
	var index = 0
	for {
		select {
		case <-after:
			return status
		case <-ctx.Done():
			return status
		default:
			event := result.AllEventList[index]
			topic := ""
			switch event.(type) {
			case nexmark.Person:
				topic = "person"
			case nexmark.Auction:
				topic = "auction"
			case nexmark.Bid:
				topic = "bid"
			}
			data, err := json.Marshal(event)
			if err != nil {
				log.Println(err)
				continue
			}
			if err := limiter.Wait(ctx); err == nil {
				token := DefaultMQTTClient.Publish(topic, 0, false, data)
				token.Wait()
			} else {
				log.Println(err)
				continue
			}
			switch event.(type) {
			case nexmark.Person:
				status.SendPersonRecordsCount++
			case nexmark.Auction:
				status.SendAuctionRecordsCount++
			case nexmark.Bid:
				status.SendBidRecordsCount++
			}
			index++
			if index >= len(result.AllEventList) {
				index = 0
			}
		}
	}
}

type RecordsSendStatus struct {
	SendPersonRecordsCount  int
	SendAuctionRecordsCount int
	SendBidRecordsCount     int
}

type Query struct {
	RecvdData             []interface{}
	TotalRecordsOut       int
	TotalRecordsProcessed int
}

func startRecvData(q *Query, topic string) {
	var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		var m interface{}
		data := msg.Payload()
		if err := json.Unmarshal(data, &m); err != nil {
			log.Println(fmt.Sprintf("recv invalid data,topic: %v,data:%v", topic, string(data)))
			return
		}
		q.RecvdData = append(q.RecvdData, m)
		switch v := m.(type) {
		case []map[string]interface{}:
			q.TotalRecordsProcessed += len(v)
			q.TotalRecordsOut++
		case map[string]interface{}:
			q.TotalRecordsProcessed++
			q.TotalRecordsOut++
		case []interface{}:
			q.TotalRecordsProcessed += len(v)
			q.TotalRecordsOut++
		}
	}
	token := DefaultMQTTClient.Subscribe(topic, 0, messagePubHandler)
	token.Wait()
}
