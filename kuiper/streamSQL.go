package kuiper

const Person = `{"sql":"create stream person (id bigint,name string,emailAddress string,creditCard string,city string,state string,datetime bigint,extra string) WITH ( datasource = \"person\", FORMAT = \"json\")"}`

const Auction = `{"sql":"create stream auction (id bigint,itemName string,description string,initialBid bigint,reserve bigint,datetime bigint,expires bigint,seller bigint,category bigint,extra string) WITH ( datasource = \"auction\", FORMAT = \"json\")"}`

const Bid = `{"sql":"create stream bid (auction bigint,bidder bigint,price bigint,channel string,url string,datetime bigint,extra string) WITH ( datasource = \"bid\", FORMAT = \"json\")"}`
