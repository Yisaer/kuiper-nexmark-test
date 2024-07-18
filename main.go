package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/yisaer/kuiper-nexmark-test/kuiper"
	"github.com/yisaer/kuiper-nexmark-test/query"
)

var (
	kuiperHost   string
	kuiperPort   int
	broker       string
	brokerPort   int
	duration     time.Duration
	qps          int
	quries       string
	bufferLength int
)

func init() {
	flag.StringVar(&kuiperHost, "host", "127.0.0.1", "kuiper host")
	flag.IntVar(&kuiperPort, "kuiperPort", 9081, "kuiper port")
	flag.StringVar(&broker, "broker", "127.0.0.1", "mqtt broker host")
	flag.IntVar(&brokerPort, "brokerPort", 1883, "mqtt broker port")
	flag.DurationVar(&duration, "duration", 1*time.Minute, "duration")
	flag.IntVar(&qps, "qps", 10, "qps")
	flag.StringVar(&quries, "quries", "q10", "queries")
	flag.IntVar(&bufferLength, "bufferLength", 1024, "concurrency")
	flag.Parse()
}

func main() {
	setupParam()
	setupMQTTConnection()
	kuiper.BrokerHost = broker
	kuiper.BrokerPort = brokerPort
	benchmark(context.Background())
}

func setupMQTTConnection() {
	c, err := query.NewMQTTClient(broker, brokerPort)
	if err != nil {
		panic(err)
	}
	query.DefaultMQTTClient = c
}

func benchmark(ctx context.Context) {
	ep := &kuiper.EKuiperEndpoint{Host: kuiperHost, Port: kuiperPort}
	kuiper.DefaultEndpoint = ep
	if err := ep.SetupSchema(); err != nil {
		panic(err)
	}
	log.Println("setup schemas success")
	for _, q := range strings.Split(quries, ",") {
		benchmarkSingleCase(ctx, q)
	}
}

func setupParam() {
	query.BufferLength = bufferLength
	kuiper.BufferLength = bufferLength
}

func benchmarkSingleCase(pctx context.Context, q string) {
	qctx, cancel := context.WithCancel(pctx)
	qc := query.Quries[q]
	if err := qc.Prepare(); err != nil {
		panic(err)
	}
	if err := qc.SetupQuery(); err != nil {
		panic(err)
	}
	defer func() {
		if err := qc.Clear(); err != nil {
			panic(err)
		}
	}()
	log.Println(fmt.Sprintf("setup query %v success", q))
	qc.StartRecvData()
	status := qc.RunWorkload(qctx, duration, qps)
	log.Println(fmt.Sprintf("query %s send records finished", q))
	recordsOut, recordsProcessed := qc.GetResult()
	cancel()
	time.Sleep(5 * time.Second)
	log.Println(buildStatusLog(q, status, recordsOut, recordsProcessed))
	log.Println(fmt.Sprintf("query %s finish", q))
}

func buildStatusLog(q string, status query.RecordsSendStatus, recordsOut, recordsProcessed int) string {
	b := bytes.NewBufferString("query ")
	b.WriteString(q)
	b.WriteString(" ")
	if status.SendPersonRecordsCount > 0 {
		b.WriteString(fmt.Sprintf("send %v person record ", status.SendPersonRecordsCount))
	}
	if status.SendAuctionRecordsCount > 0 {
		b.WriteString(fmt.Sprintf("send %v auction record ", status.SendAuctionRecordsCount))
	}
	if status.SendBidRecordsCount > 0 {
		b.WriteString(fmt.Sprintf("send %v bid record ", status.SendBidRecordsCount))
	}
	b.WriteString(fmt.Sprintf("sink %v records out, %v records processed", recordsOut, recordsProcessed))
	return b.String()
}
