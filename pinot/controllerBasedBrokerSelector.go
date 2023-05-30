package pinot

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	controllerAPIEndpoint = "/v2/brokers/tables?state=ONLINE"
	defaultUpdateFreqMs   = 1000
)

var (
	controllerDefaultHTTPHeader = map[string]string{
		"Accept": "application/json",
	}
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type controllerBasedSelector struct {
	client              HTTPClient
	config              *ControllerConfig
	controllerAPIReqUrl string
	tableAwareBrokerSelector
}

func (s *controllerBasedSelector) init() error {
	if s.config.UpdateFreqMs == 0 {
		s.config.UpdateFreqMs = defaultUpdateFreqMs
	}
	u, err := getControllerRequestUrl(s.config.ControllerAddress)
	if err != nil {
		log.Error(err)
		return err
	}
	s.controllerAPIReqUrl = u

	err = s.updateBrokerData()
	if err != nil {
		log.Errorf(
			"An error occurred when fetching broker data from controller API for the first time, Error: %v",
			err,
		)
		return err
	}
	go s.setupInterval()
	return nil
}

func (s *controllerBasedSelector) setupInterval() {
	lastInvocation := time.Now()
	for {
		nextInvocation := lastInvocation.Add(
			time.Duration(s.config.UpdateFreqMs) * time.Millisecond,
		)
		untilNextInvocation := time.Until(nextInvocation)
		time.Sleep(untilNextInvocation)

		err := s.updateBrokerData()
		if err != nil {
			log.Errorf("Caught exception when updating broker data, Error: %v", err)
		}

		lastInvocation = time.Now()
	}
}

func getControllerRequestUrl(controllerAddress string) (string, error) {
	tokenized := strings.Split(controllerAddress, "://")
	addressWithScheme := controllerAddress
	if len(tokenized) > 1 {
		scheme := tokenized[0]
		if scheme != "https" && scheme != "http" {
			return "", fmt.Errorf(
				"Unsupported controller URL scheme: %s, only http (default) and https are allowed",
				scheme,
			)
		}
	} else {
		addressWithScheme = "http://" + controllerAddress
	}
	return strings.TrimSuffix(addressWithScheme, "/") + controllerAPIEndpoint, nil
}

func (s *controllerBasedSelector) createControllerRequest() (*http.Request, error) {
	r, err := http.NewRequest("GET", s.controllerAPIReqUrl, nil)
	if err != nil {
		return r, fmt.Errorf(
			"Caught exception when creating controller API request: %v",
			err,
		)
	}
	for k, v := range controllerDefaultHTTPHeader {
		r.Header.Add(k, v)
	}
	for k, v := range s.config.ExtraControllerAPIHeaders {
		r.Header.Add(k, v)
	}
	return r, nil
}

func (s *controllerBasedSelector) updateBrokerData() error {
	r, err := s.createControllerRequest()
	if err != nil {
		return err
	}
	resp, err := s.client.Do(r)
	if err != nil {
		return fmt.Errorf("Got exceptions while sending controller API request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("An error occurred when reading controller API response: %v", err)
		}
		var c controllerResponse
		if err = decodeJsonWithNumber(bodyBytes, &c); err != nil {
			return fmt.Errorf("An error occurred when decoding controller API response: %v", err)
		}
		s.rwMux.Lock()
		s.allBrokerList = c.extractBrokerList()
		s.tableBrokerMap = c.extractTableToBrokerMap()
		s.rwMux.Unlock()
		return nil
	}
	return fmt.Errorf("Controller API returned HTTP status code %v", resp.StatusCode)
}
