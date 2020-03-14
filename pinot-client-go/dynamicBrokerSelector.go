package pinot

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	zk "github.com/samuel/go-zookeeper/zk"

	log "github.com/sirupsen/logrus"
)

const (
	brokerExternalViewPath = "EXTERNALVIEW/brokerResource"
	offlineSuffix          = "_OFFLINE"
	realtimeSuffix         = "_REALTIME"
)

type dynamicBrokerSelector struct {
	zkConfig       *ZookeeperConfig
	zkConn         *zk.Conn
	tableBrokerMap map[string]([]string)
	allBrokerList  []string
}

type externalView struct {
	ID           string                         `json:"id"`
	SimpleFields map[string]string              `json:"simpleFields"`
	MapFields    map[string](map[string]string) `json:"mapFields"`
	ListFields   map[string]([]string)          `json:"listFields"`
}

func (s *dynamicBrokerSelector) init() error {
	var err error
	s.zkConn, _, err = zk.Connect(s.zkConfig.ZookeeperPath, time.Duration(s.zkConfig.SessionTimeoutSec)*time.Second)
	if err != nil {
		log.Errorf("Failed to connect to zookeeper: %v\n", s.zkConfig.ZookeeperPath)
		return err
	}

	node, stat, znodeWatch, err := s.zkConn.GetW(s.zkConfig.PathPrefix + "/" + brokerExternalViewPath)
	if err != nil {
		log.Errorf("Failed to set a watcher on zk, ExternalView: %v\n", s.zkConfig.ZookeeperPath)
		log.Errorf("Failed to set a watcher on zk, Error: %v\n", err)
		return err
	}
	log.Debugf("znode status for brokerExternalViewPath: %+s %+v\n", node, stat)
	if err = s.refreshExternalView(); err != nil {
		log.Errorf("Failed to refresh ExternalView: %v\n", err)
		return err
	}
	go func() {
		for {
			select {
			case ev := <-znodeWatch:
				if ev.Err != nil {
					log.Error("GetW watcher error", ev.Err)
				} else if ev.Type == zk.EventNodeDataChanged {
					s.refreshExternalView()
					if err != nil {
						log.Errorf("Failed to refresh ExternalView: %v\n", err)
					}
				}
				break
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()
	return nil
}

func (s *dynamicBrokerSelector) refreshExternalView() error {
	node, _, err := s.zkConn.Get(s.zkConfig.PathPrefix + "/" + brokerExternalViewPath)
	if err != nil {
		log.Errorf("Failed to read zk ExternalView node: %v\n", s.zkConfig.ZookeeperPath)
		return err
	}
	var ev externalView
	if err = json.Unmarshal(node, &ev); err != nil {
		log.Errorf("Failed to unmarshal ExternalView: %s, Error: %v\n", node, err)
		return err
	}
	jsonBytes, _ := json.Marshal(ev)
	log.Debugf("JSON Marshal externalView: %s", jsonBytes)
	newTableBrokerMap := map[string]([]string){}
	newAllBrokerList := []string{}
	for table, brokerMapping := range ev.MapFields {
		tableName := extractTableName(table)
		newTableBrokerMap[tableName] = extractBrokers(brokerMapping)
		newAllBrokerList = append(newAllBrokerList, newTableBrokerMap[tableName]...)
	}
	s.tableBrokerMap = newTableBrokerMap
	s.allBrokerList = newAllBrokerList

	log.Debugf("Updated tableBrokerMap = %v", s.tableBrokerMap)
	log.Debugf("Updated allBrokerList = %v", s.allBrokerList)
	return nil
}

func (s *dynamicBrokerSelector) selectBroker(table string) (string, error) {
	tableName := extractTableName(table)
	var brokerList []string
	if tableName == "" {
		brokerList = s.allBrokerList
		if len(brokerList) == 0 {
			return "", fmt.Errorf("No availble broker found")
		}
	} else {
		var found bool
		brokerList, found = s.tableBrokerMap[tableName]
		if !found {
			return "", fmt.Errorf("Unable to find the table: %s", table)
		}
		if len(brokerList) == 0 {
			return "", fmt.Errorf("No availble broker found for table: %s", table)
		}
	}
	return brokerList[rand.Intn(len(brokerList))], nil
}

func extractTableName(table string) string {
	return strings.Replace(strings.Replace(table, offlineSuffix, "", 1), realtimeSuffix, "", 1)
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
