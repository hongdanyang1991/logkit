package sender

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"

	elasticV6 "github.com/olivere/elastic"
	"github.com/qiniu/logkit/times"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"
)

// ElasticsearchSender ElasticSearch sender
type ElasticsearchSender struct {
	name string

	host            []string
	retention       int
	indexName       []string
	eType           string
	eVersion        string
	elasticV3Client *elasticV3.Client
	elasticV5Client *elasticV5.Client
	elasticV6Client *elasticV6.Client

	aliasFields map[string]string

	intervalIndex  int
	timeZone       *time.Location
	logkitSendTime bool
	timestamp      string
}

const (
	KeyElasticHost    = "elastic_host"
	KeyElasticVersion = "elastic_version"
	KeyElasticIndex   = "elastic_index" //index 1.填一个值,则index为所填值 2.填两个值: %{[字段名]}, defaultIndex :根据每条event,以指定字段值为index,若无,则用默认值
	KeyElasticType    = "elastic_type"
	KeyElasticAlias   = "elastic_keys"

	KeyElasticIndexStrategy = "elastic_index_strategy"
	KeyElasticTimezone      = "elastic_time_zone"
	keyElasticTimestamp     = "elastic_timestamp" //指定时间戳字段  1.若为空,则不指定 2.若某条数据不存在该字段,则创建,并以当前时间为value 3.若某条数据存在该字段且无法转换成时间类型,则丢弃该条数据
)

const (
	KeyDefaultIndexStrategy = "default"
	KeyYearIndexStrategy    = "year"
	KeyMonthIndexStrategy   = "month"
	KeyDayIndexStrategy     = "day"
)

var (
	// ElasticVersion3 v3.x
	ElasticVersion3 = "3.x"
	// ElasticVersion5 v5.x
	ElasticVersion5 = "5.x"
	// ElasticVersion6 v6.x
	ElasticVersion6 = "6.x"
)

//timeZone
const (
	KeyLocalTimezone   = "Local"
	KeyUTCTimezone     = "UTC"
	KeyPRCTimezone     = "PRC"
	KeyDefaultTimezone = KeyUTCTimezone
)

const KeySendTime = "sendTime"

// NewElasticSender New ElasticSender
func NewElasticSender(conf conf.MapConf) (sender Sender, err error) {
	host, err := conf.GetStringList(KeyElasticHost)
	if err != nil {
		return
	}
	for i, h := range host {
		if !strings.HasPrefix(h, "http://") {
			host[i] = fmt.Sprintf("http://%s", h)
		}
	}

	index, err := conf.GetStringList(KeyElasticIndex)
	if err != nil {
		return
	}

	index, err = ExtractField(index)
	if err != nil {
		return
	}

	// 索引后缀模式
	indexStrategy, _ := conf.GetStringOr(KeyElasticIndexStrategy, KeyDefaultIndexStrategy)
	timezone, _ := conf.GetStringOr(KeyElasticTimezone, KeyDefaultTimezone)
	timeZone, err := time.LoadLocation(timezone)
	if err != nil {
		return
	}
	logkitSendTime, _ := conf.GetBoolOr(KeyLogkitSendTime, true)
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))
	eVersion, _ := conf.GetStringOr(KeyElasticVersion, ElasticVersion3)
	timestamp, _ := conf.GetString(keyElasticTimestamp)

	strategy := []string{KeyDefaultIndexStrategy, KeyYearIndexStrategy, KeyMonthIndexStrategy, KeyDayIndexStrategy}

	i, err := machPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	// 初始化 client
	var elasticV3Client *elasticV3.Client
	var elasticV5Client *elasticV5.Client
	var elasticV6Client *elasticV6.Client
	switch eVersion {
	case ElasticVersion6:
		elasticV6Client, err = elasticV6.NewClient(
			elasticV6.SetSniff(false),
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(host...))
		if err != nil {
			return
		}
	case ElasticVersion5:
		elasticV5Client, err = elasticV5.NewClient(
			elasticV5.SetSniff(false),
			elasticV5.SetHealthcheck(false),
			elasticV5.SetURL(host...))
		if err != nil {
			return
		}
	default:
		elasticV3Client, err = elasticV3.NewClient(elasticV3.SetURL(host...))
		if err != nil {
			return
		}
	}

	return &ElasticsearchSender{
		name:            name,
		host:            host,
		indexName:       index,
		eVersion:        eVersion,
		elasticV3Client: elasticV3Client,
		elasticV5Client: elasticV5Client,
		elasticV6Client: elasticV6Client,
		eType:           eType,
		aliasFields:     fields,
		intervalIndex:   i,
		timeZone:        timeZone,
		logkitSendTime:  logkitSendTime,
		timestamp:       timestamp,
	}, nil
}

const defaultType string = "logkit"

// machPattern 判断字符串是否符合已有的模式
func machPattern(s string, strategys []string) (i int, err error) {
	for i, strategy := range strategys {
		if s == strategy {
			return i, err
		}
	}
	err = fmt.Errorf("unknown index_strategy: '%s'", s)
	return i, err
}

// Name ElasticSearchSenderName
func (ess *ElasticsearchSender) Name() string {
	return ess.name
}

// Send ElasticSearchSender
func (ess *ElasticsearchSender) Send(data []Data) (err error) {
	switch ess.eVersion {
	case ElasticVersion6:
		bulkService := ess.elasticV6Client.Bulk()

		makeDoc := true
		if len(ess.aliasFields) == 0 {
			makeDoc = false
		}
		var indexName string
		for _, doc := range data {

			//加工字段
			if err = processDoc(ess, doc); err != nil {
				continue
			}
			//计算索引
			if indexName, err = buildIndexName(ess, doc, ess.indexName, ess.timeZone, ess.intervalIndex); err != nil {
				continue
			}
			//字段名称替换
			if makeDoc {
				doc = ess.wrapDoc(doc)
			}
			//添加发送时间
			if ess.logkitSendTime {
				doc[KeySendTime] = time.Now().In(ess.timeZone)
			}
			doc2 := doc
			bulkService.Add(elasticV6.NewBulkIndexRequest().Index(indexName).Type(ess.eType).Doc(&doc2))
		}

		_, err = bulkService.Do(context.Background())
		if err != nil {
			return
		}
	case ElasticVersion5:
		bulkService := ess.elasticV5Client.Bulk()

		makeDoc := true
		if len(ess.aliasFields) == 0 {
			makeDoc = false
		}
		var indexName string
		for _, doc := range data {
			//加工字段
			if err = processDoc(ess, doc); err != nil {
				continue
			}
			//计算索引
			if indexName, err = buildIndexName(ess, doc, ess.indexName, ess.timeZone, ess.intervalIndex); err != nil {
				continue
			}
			//字段名称替换
			if makeDoc {
				doc = ess.wrapDoc(doc)
			}
			//添加发送时间
			if ess.logkitSendTime {
				doc[KeySendTime] = time.Now().In(ess.timeZone)
			}
			doc2 := doc
			bulkService.Add(elasticV5.NewBulkIndexRequest().Index(indexName).Type(ess.eType).Doc(&doc2))
		}

		_, err = bulkService.Do(context.Background())
		if err != nil {
			return
		}
	default:
		bulkService := ess.elasticV3Client.Bulk()

		makeDoc := true
		if len(ess.aliasFields) == 0 {
			makeDoc = false
		}
		var indexName string
		for _, doc := range data {
			//加工字段
			if err = processDoc(ess, doc); err != nil {
				continue
			}
			//计算索引
			if indexName, err = buildIndexName(ess, doc, ess.indexName, ess.timeZone, ess.intervalIndex); err != nil {
				continue
			}
			//字段名称替换
			if makeDoc {
				doc = ess.wrapDoc(doc)
			}
			//添加发送时间
			if ess.logkitSendTime {
				doc[KeySendTime] = time.Now().In(ess.timeZone)
			}
			doc2 := doc
			bulkService.Add(elasticV3.NewBulkIndexRequest().Index(indexName).Type(ess.eType).Doc(&doc2))
		}

		_, err = bulkService.Do()
		if err != nil {
			return
		}
	}
	return
}

func processDoc(ess *ElasticsearchSender, doc Data) error {
	if ess.timestamp != "" {
		if _, ok := doc[ess.timestamp]; ok {
			//验证是否为日期字符串
			if timeStr, ok := doc[ess.timestamp].(string); ok {
				timestamp, err := times.StrToTime(timeStr)
				if err != nil {
					return err
				}
				doc[ess.timestamp] = timestamp.In(ess.timeZone).Format(time.RFC3339Nano)
			} else {
				return fmt.Errorf("appointed timestamp field: %v is not type of string", doc[ess.timestamp])
			}
		} else {
			doc[ess.timestamp] = time.Now().In(ess.timeZone).Format(time.RFC3339Nano)
		}
	}
	return nil
}

func buildIndexName(ess *ElasticsearchSender, data Data, index []string, timeZone *time.Location, size int) (string, error) {
	var indexName string
	var timestamp time.Time
	var err error
	if ess.timestamp == "" || data[ess.timestamp] == "" {
		timestamp = time.Now()
	} else {
		if timeStr, ok := data[ess.timestamp].(string); ok {
			if timestamp, err = times.StrToTime(timeStr); err != nil {
				return "", err
			}
		} else {
			return "", fmt.Errorf("timestamp %v is not timeStr", data[ess.timestamp])
		}
	}
	timestamp = timestamp.In(timeZone)
	intervals := []string{strconv.Itoa(timestamp.Year()), strconv.Itoa(int(timestamp.Month())), strconv.Itoa(timestamp.Day())}
	if len(index) == 2 {
		if data[index[0]] == nil || data[index[0]] == "" {
			indexName = index[1]
		} else {
			if myIndexName, ok := data[index[0]].(string); ok {
				indexName = myIndexName
			} else {
				indexName = index[1]
			}
		}
	} else {
		indexName = index[0]
	}

	if err = checkESIndexLegal(&indexName); err != nil {
		return "", fmt.Errorf("given elasticSearch indexName is illegal")
	}

	for j := 0; j < size; j++ {
		if j == 0 {
			indexName = indexName + "-" + intervals[j]
		} else {
			if len(intervals[j]) == 1 {
				intervals[j] = "0" + intervals[j]
			}
			indexName = indexName + "." + intervals[j]
		}
	}
	return indexName, nil
}

//检测elasticsearch名称是否合法,并将字符转换成小写
func checkESIndexLegal(indexName *string) error {
	*indexName = strings.ToLower(*indexName)
	return nil
}

// Close ElasticSearch Sender Close
func (ess *ElasticsearchSender) Close() error {
	return nil
}

func (ess *ElasticsearchSender) wrapDoc(doc map[string]interface{}) map[string]interface{} {
	//newDoc := make(map[string]interface{})
	for oldKey, newKey := range ess.aliasFields {
		val, ok := doc[oldKey]
		if ok {
			//newDoc[newKey] = val
			delete(doc, oldKey)
			doc[newKey] = val
			continue
		}
		log.Errorf("key %s not found in doc", oldKey)
	}
	//return newDoc
	return doc
}
