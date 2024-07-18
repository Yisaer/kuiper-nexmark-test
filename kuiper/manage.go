package kuiper

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
)

var DefaultEndpoint *EKuiperEndpoint
var BrokerHost string
var BrokerPort int
var BufferLength int

type EKuiperEndpoint struct {
	Host string
	Port int
}

func (e *EKuiperEndpoint) SetupSchema() error {
	s := []string{"person", "auction", "bid"}
	for _, schemaName := range s {
		if err := e.DropStream(schemaName); err != nil {
			log.Println(fmt.Sprintf("drop stream %s failed, err:%v", schemaName, err))
			return err
		}
	}
	if err := e.CreateStream(Person); err != nil {
		log.Println(fmt.Sprintf("create stream person failed, err:%v", err))
		return err
	}
	if err := e.CreateStream(Auction); err != nil {
		log.Println(fmt.Sprintf("create stream auction failed, err:%v", err))
		return err
	}
	if err := e.CreateStream(Bid); err != nil {
		log.Println(fmt.Sprintf("create stream bid failed, err:%v", err))
		return err
	}
	return nil
}

func (e *EKuiperEndpoint) DropQuery(query string) error {
	if err := e.DropRule(query); err != nil {
		log.Println(fmt.Sprintf("drop query %s failed, err:%v", query, err))
		return err
	}
	return nil
}

func (e *EKuiperEndpoint) CreateRuleByJSON(ruleJSON string) error {
	r, err := http.Post(fmt.Sprintf("http://%s:%d/rules", e.Host, e.Port), "application/json; charset=UTF-8", bytes.NewReader([]byte(ruleJSON)))
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (e *EKuiperEndpoint) CreateRawRule(ruleJson string) error {
	r, err := http.Post(fmt.Sprintf("http://%s:%d/rules", e.Host, e.Port), "application/json; charset=UTF-8", bytes.NewReader([]byte(ruleJson)))
	if err != nil {
		return err
	}
	r.Body.Close()
	if r.StatusCode != http.StatusCreated {
		return fmt.Errorf("create raw rule failed, status code:%v", r.StatusCode)
	}
	return nil
}

func (e *EKuiperEndpoint) CreateRule(id, sql string) error {
	data := fmt.Sprintf(`{
 "id": "%s",
 "sql": "%s",
 "actions": [
   {
    "mqtt": {
       "server": "tcp://%s:%v",
       "topic": "%s"
     }
   }
 ],
 "options": {
        "bufferLength": %v
    }
}`, id, sql, BrokerHost, BrokerPort, id, BufferLength)
	r, err := http.Post(fmt.Sprintf("http://%s:%d/rules", e.Host, e.Port), "application/json; charset=UTF-8", bytes.NewReader([]byte(data)))
	if err != nil {
		return err
	}
	r.Body.Close()
	if r.StatusCode != http.StatusCreated {
		return fmt.Errorf("create raw rule failed, status code:%v", r.StatusCode)
	}
	return nil
}

func (e *EKuiperEndpoint) CreateStream(data string) error {
	r, err := http.Post(fmt.Sprintf("http://%s:%d/streams", e.Host, e.Port), "application/json; charset=UTF-8", bytes.NewReader([]byte(data)))
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (e *EKuiperEndpoint) DropRule(id string) error {
	r, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s:%d/rules/%s", e.Host, e.Port, id), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (e *EKuiperEndpoint) DropStream(name string) error {
	r, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s:%d/streams/%s", e.Host, e.Port, name), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

type BidStatus struct {
	RecordsOut int `json:"sink_log_0_0_records_out_total"`
}
