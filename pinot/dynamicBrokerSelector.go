package pinot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	zk "github.com/go-zookeeper/zk"

	log "github.com/sirupsen/logrus"
)

const (
	brokerExternalViewPath = "EXTERNALVIEW/brokerResource"
)

type ReadZNode func(path string) ([]byte, error)

type dynamicBrokerSelector struct {
	zkConfig               *ZookeeperConfig
	zkConn                 *zk.Conn
	externalViewZnodeWatch <-chan zk.Event
	readZNode              ReadZNode
	externalViewZkPath     string
	tableAwareBrokerSelector
}

type externalView struct {
	SimpleFields map[string]string              `json:"simpleFields"`
	MapFields    map[string](map[string]string) `json:"mapFields"`
	ListFields   map[string]([]string)          `json:"listFields"`
	ID           string                         `json:"id"`
}

func (s *dynamicBrokerSelector) init() error {
	var err error
	s.zkConn, _, err = zk.Connect(s.zkConfig.ZookeeperPath, time.Duration(s.zkConfig.SessionTimeoutSec)*time.Second)
	if err != nil {
		log.Errorf("Failed to connect to zookeeper: %v\n", s.zkConfig.ZookeeperPath)
		return err
	}
	s.readZNode = func(path string) ([]byte, error) {
		if s.zkConn == nil {
			return nil, fmt.Errorf("Zk Connection hasn't been initialized.")
		}
		node, _, err := s.zkConn.Get(s.externalViewZkPath)
		if err != nil {
			log.Errorf("Failed to read zk: %s, ExternalView path: %s\n", s.zkConfig.ZookeeperPath, s.externalViewZkPath)
			return nil, err
		}
		return node, nil
	}
	s.externalViewZkPath = s.zkConfig.PathPrefix + "/" + brokerExternalViewPath
	_, _, s.externalViewZnodeWatch, err = s.zkConn.GetW(s.externalViewZkPath)
	if err != nil {
		log.Errorf("Failed to set a watcher on ExternalView path: %s, Error: %v\n", strings.Join(append(s.zkConfig.ZookeeperPath, s.externalViewZkPath), ""), err)
		return err
	}
	if err = s.refreshExternalView(); err != nil {
		return err
	}
	go s.setupWatcher()
	return nil
}

func (s *dynamicBrokerSelector) setupWatcher() {
	for {
		select {
		case ev := <-s.externalViewZnodeWatch:
			if ev.Err != nil {
				log.Error("GetW watcher error", ev.Err)
			} else if ev.Type == zk.EventNodeDataChanged {
				s.refreshExternalView()
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *dynamicBrokerSelector) refreshExternalView() error {
	if s.readZNode == nil {
		return fmt.Errorf("No method defined to read from a ZNode.")
	}
	node, err := s.readZNode(s.externalViewZkPath)
	if err != nil {
		return err
	}
	ev, err := getExternalView(node)
	if err != nil {
		return err
	}
	newTableBrokerMap, newAllBrokerList := generateNewBrokerMappingExternalView(ev)
	s.rwMux.Lock()
	s.tableBrokerMap = newTableBrokerMap
	s.allBrokerList = newAllBrokerList
	s.rwMux.Unlock()
	return nil
}

func getExternalView(evBytes []byte) (*externalView, error) {
	var ev externalView
	if err := json.Unmarshal(evBytes, &ev); err != nil {
		log.Errorf("Failed to unmarshal ExternalView: %s, Error: %v\n", evBytes, err)
		return nil, err
	}
	return &ev, nil
}

func generateNewBrokerMappingExternalView(ev *externalView) (map[string]([]string), []string) {
	tableBrokerMap := map[string]([]string){}
	allBrokerList := []string{}
	for table, brokerMapping := range ev.MapFields {
		tableName := extractTableName(table)
		tableBrokerMap[tableName] = extractBrokers(brokerMapping)
		allBrokerList = append(allBrokerList, tableBrokerMap[tableName]...)
	}
	return tableBrokerMap, allBrokerList
}

func extractBrokers(brokerMap map[string]string) []string {
	brokerList := []string{}
	for brokerName, status := range brokerMap {
		if status == "ONLINE" {
			host, port, err := extractBrokerHostPort(brokerName)
			if err == nil {
				brokerList = append(brokerList, strings.Join([]string{host, port}, ":"))
			}
		}
	}
	return brokerList
}

func extractBrokerHostPort(brokerKey string) (string, string, error) {
	splits := strings.Split(brokerKey, "_")
	if len(splits) < 2 {
		err := fmt.Errorf("Invalid Broker Key: %s, should be in the format of Broker_[hostname]_[port]", brokerKey)
		log.Error(err)
		return "", "", err
	}
	_, err := strconv.Atoi(splits[len(splits)-1])
	if err != nil {
		log.Errorf("Failed to parse broker port:%s to integer", splits[len(splits)-1])
		return "", "", err
	}
	return splits[len(splits)-2], splits[len(splits)-1], nil
}
