package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
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
	queries      string
	bufferLength int
	parallel     bool
)

func init() {
	flag.StringVar(&kuiperHost, "host", "127.0.0.1", "kuiper host")
	flag.IntVar(&kuiperPort, "kuiperPort", 9081, "kuiper port")
	flag.StringVar(&broker, "broker", "127.0.0.1", "mqtt broker host")
	flag.IntVar(&brokerPort, "brokerPort", 1883, "mqtt broker port")
	flag.DurationVar(&duration, "duration", 5*time.Second, "duration")
	flag.IntVar(&qps, "qps", 10, "qps")
	flag.StringVar(&queries, "queries", "q1", "queries")
	flag.IntVar(&bufferLength, "bufferLength", 1024, "concurrency")
	flag.BoolVar(&parallel, "parallel", false, "parallel running queries")
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
	qtests := strings.Split(queries, ",")
	if !parallel {
		for _, q := range qtests {
			log.Println("start running query " + q)
			result := benchmarkSingleCase(ctx, q)
			log.Println(fmt.Sprintf("result: %v", buildStatusLog(result)))
		}
		return
	} else {
		wg := sync.WaitGroup{}
		results := make([]benchmarkResult, len(qtests), len(qtests))
		log.Println("parallel running queries ", qtests)
		for i, q := range qtests {
			wg.Add(1)
			go func(index int, query string) {
				defer func() {
					wg.Done()
				}()
				result := benchmarkSingleCase(ctx, query)
				results[index] = result
			}(i, q)
		}
		wg.Wait()
		log.Println("finish running queries ", qtests)
		for _, result := range results {
			log.Println(buildStatusLog(result))
		}
	}
}

func setupParam() {
	query.BufferLength = bufferLength
	kuiper.BufferLength = bufferLength
}

type benchmarkResult struct {
	query            string
	sendStatus       query.RecordsSendStatus
	recordsOut       int
	recordsProcessed int
}

func benchmarkSingleCase(pctx context.Context, q string) benchmarkResult {
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
	time.Sleep(1 * time.Second)
	return benchmarkResult{
		query:            q,
		sendStatus:       status,
		recordsOut:       recordsOut,
		recordsProcessed: recordsProcessed,
	}
}

func buildStatusLog(result benchmarkResult) string {
	b := bytes.NewBufferString("query ")
	b.WriteString(result.query)
	b.WriteString(" ")
	if result.sendStatus.SendPersonRecordsCount > 0 {
		b.WriteString(fmt.Sprintf("send %v person record ", result.sendStatus.SendPersonRecordsCount))
	}
	if result.sendStatus.SendAuctionRecordsCount > 0 {
		b.WriteString(fmt.Sprintf("send %v auction record ", result.sendStatus.SendAuctionRecordsCount))
	}
	if result.sendStatus.SendBidRecordsCount > 0 {
		b.WriteString(fmt.Sprintf("send %v bid record ", result.sendStatus.SendBidRecordsCount))
	}
	b.WriteString(fmt.Sprintf("sink %v records out, %v records processed", result.recordsOut, result.recordsProcessed))
	return b.String()
}
