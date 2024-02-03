package pinot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	defaultHTTPHeader = map[string]string{
		"Content-Type": "application/json; charset=utf-8",
	}
)

// jsonAsyncHTTPClientTransport is the impl of clientTransport
type jsonAsyncHTTPClientTransport struct {
	client *http.Client
	header map[string]string
}

func (t jsonAsyncHTTPClientTransport) execute(brokerAddress string, query *Request) (*BrokerResponse, error) {
	url := fmt.Sprintf(getQueryTemplate(query.queryFormat, brokerAddress), brokerAddress)
	requestJSON := map[string]string{}
	requestJSON[query.queryFormat] = query.query
	queryOptions := ""
	if query.queryFormat == "sql" {
		queryOptions = "groupByMode=sql;responseFormat=sql"
	}
	if query.useMultistageEngine {
		if queryOptions != "" {
			queryOptions += ";"
		}
		queryOptions += "useMultistageEngine=true"
	}
	if queryOptions != "" {
		requestJSON["queryOptions"] = queryOptions
	}
	if query.trace {
		requestJSON["trace"] = "true"
	}
	jsonValue, err := json.Marshal(requestJSON)
	if err != nil {
		log.Error("Unable to marshal request to JSON. ", err)
		return nil, err
	}
	req, err := createHTTPRequest(url, jsonValue, t.header)
	if err != nil {
		return nil, err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		log.Error("Got exceptions during sending request. ", err)
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Error("Got exceptions during closing response body. ", err)
		}
	}()
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Error("Unable to read Pinot response. ", err)
			return nil, err
		}
		var brokerResponse BrokerResponse
		if err = decodeJSONWithNumber(bodyBytes, &brokerResponse); err != nil {
			log.Error("Unable to unmarshal json response to a brokerResponse structure. ", err)
			return nil, err
		}
		return &brokerResponse, nil
	}
	return nil, fmt.Errorf("caught http exception when querying Pinot: %v", resp.Status)
}

func getQueryTemplate(queryFormat string, brokerAddress string) string {
	if queryFormat == "sql" {
		if strings.HasPrefix(brokerAddress, "http://") || strings.HasPrefix(brokerAddress, "https://") {
			return "%s/query/sql"
		}
		return "http://%s/query/sql"
	}
	if strings.HasPrefix(brokerAddress, "http://") || strings.HasPrefix(brokerAddress, "https://") {
		return "%s/query"
	}
	return "http://%s/query"
}

func createHTTPRequest(url string, jsonValue []byte, extraHeader map[string]string) (*http.Request, error) {
	r, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		log.Error("Invalid HTTP Request", err)
		return nil, err
	}
	for k, v := range defaultHTTPHeader {
		r.Header.Add(k, v)
	}
	for k, v := range extraHeader {
		r.Header.Add(k, v)
	}
	return r, nil
}
