package pinot

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockHTTPClientSuccess struct {
	body       io.ReadCloser
	statusCode int
}

func (m *MockHTTPClientSuccess) Do(req *http.Request) (*http.Response, error) {
	r := &http.Response{}
	r.StatusCode = m.statusCode
	r.Body = m.body
	return r, nil
}

type MockHTTPClientFailure struct {
	err error
}

func (m *MockHTTPClientFailure) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{}, m.err
}

func TestControllerBasedBrokerSelectorInit(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
		},
		client: &MockHTTPClientSuccess{
			statusCode: 200,
			body:       ioutil.NopCloser(strings.NewReader("{}")),
		},
	}
	err := s.init()
	assert.Nil(t, err)
	assert.Equal(t, s.config.UpdateFreqMs, 1000)
	assert.Equal(t, s.controllerAPIReqUrl, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
	assert.ElementsMatch(t, s.allBrokerList, []string{})
}

func TestControllerBasedBrokerSelectorInitError(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "https://host:9000",
		},
		client: &MockHTTPClientFailure{
			err: errors.New("http client error"),
		},
	}
	err := s.init()
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "http client error"))
}

func TestGetControllerRequestUrl(t *testing.T) {
	u, err := getControllerRequestUrl("localhost:9000")
	assert.Nil(t, err)
	assert.Equal(t, "http://localhost:9000/v2/brokers/tables?state=ONLINE", u)

	u, err = getControllerRequestUrl("https://host:1234")
	assert.Nil(t, err)
	assert.Equal(t, "https://host:1234/v2/brokers/tables?state=ONLINE", u)

	u, err = getControllerRequestUrl("http://host:1234")
	assert.Nil(t, err)
	assert.Equal(t, "http://host:1234/v2/brokers/tables?state=ONLINE", u)

	u, err = getControllerRequestUrl("smb://nope:1234")
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Unsupported controller URL scheme: smb"))
}

func TestCreateControllerRequest(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
			ExtraControllerAPIHeaders: map[string]string{
				"foo1": "bar",
				"foo2": "baz",
			},
		},
	}
	r, err := s.createControllerRequest()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(r.Header))
	assert.Equal(t, "bar", r.Header.Get("foo1"))
}

func TestUpdateBrokerData(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
		},
		client: &MockHTTPClientSuccess{
			statusCode: 200,
			body: ioutil.NopCloser(
				strings.NewReader(
					`{"baseballStats":[{"port":8000,"host":"172.17.0.2","instanceName":"Broker_172.17.0.2_8000"}]}`,
				),
			),
		},
	}
	err := s.updateBrokerData()
	expectedTableBrokerMap := map[string]([]string){
		"baseballStats": {
			"172.17.0.2:8000",
		},
	}
	assert.Nil(t, err)
	assert.ElementsMatch(t, s.allBrokerList, []string{"172.17.0.2:8000"})
	assert.True(t, reflect.DeepEqual(s.tableBrokerMap, expectedTableBrokerMap))
}

func TestUpdateBrokerDataHTTPError(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
		},
		client: &MockHTTPClientFailure{
			err: errors.New("http error"),
		},
	}
	s.allBrokerList = []string{"broker1:8000", "broker2:8000"}
	s.tableBrokerMap = map[string]([]string){
		"table1": {
			"broker1:8000",
			"broker2:8000",
		},
	}
	err := s.updateBrokerData()
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "http error"))
	assert.ElementsMatch(t, s.allBrokerList, []string{"broker1:8000", "broker2:8000"})
	assert.True(t, reflect.DeepEqual(s.tableBrokerMap, map[string]([]string){
		"table1": {
			"broker1:8000",
			"broker2:8000",
		},
	}))
}

func TestUpdateBrokerDataDecodeError(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
		},
		client: &MockHTTPClientSuccess{
			statusCode: 200,
			body:       ioutil.NopCloser(strings.NewReader("{not a valid json")),
		},
	}
	err := s.updateBrokerData()
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "decoding controller API response"))
}

type errReader int

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("test read error")
}

func (errReader) Close() error {
	return nil
}

func TestUpdateBrokerDataResponseReadError(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
		},
		client: &MockHTTPClientSuccess{
			statusCode: 200,
			body:       errReader(0),
		},
	}
	err := s.updateBrokerData()
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "reading controller API response"))
}

func TestUpdateBrokerDataUnexpectedHTTPStatus(t *testing.T) {
	s := &controllerBasedSelector{
		config: &ControllerConfig{
			ControllerAddress: "localhost:9000",
		},
		client: &MockHTTPClientSuccess{
			statusCode: 500,
			body:       ioutil.NopCloser(strings.NewReader("{}")),
		},
	}
	err := s.updateBrokerData()
	assert.NotNil(t, err)
	fmt.Println(err.Error())
	assert.True(t, strings.Contains(err.Error(), "returned HTTP status code 500"))
}
