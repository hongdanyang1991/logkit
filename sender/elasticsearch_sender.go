package sender

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	elasticV6 "github.com/olivere/elastic"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"

	"github.com/qiniu/log"
	config "github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/times"
	"os"
)

// ElasticsearchSender ElasticSearch sender
type ElasticsearchSender struct {
	name string

	host            []string
	retention       int
	indexName       string
	eType           string
	eVersion        string
	elasticV3Client *elasticV3.Client
	elasticV5Client *elasticV5.Client
	elasticV6Client *elasticV6.Client

	aliasFields map[string]string

	intervalIndex  int
	timeZone       *time.Location
	logkitSendTime bool
}

const (
	KeyElasticHost    = "elastic_host"
	KeyElasticVersion = "elastic_version"
	KeyElasticIndex   = "elastic_index"
	KeyElasticType    = "elastic_type"
	KeyElasticAlias   = "elastic_keys"

	KeyElasticIndexStrategy = "elastic_index_strategy"
	KeyElasticTimezone      = "elastic_time_zone"
)

const (
	KeyDefaultIndexStrategy = "default"
	KeyYearIndexStrategy    = "year"
	KeyMonthIndexStrategy   = "month"
	KeyDayIndexStrategy     = "day"
)

const (
	KeyAuthUsername = "auth_username"
	KeyAuthPassword = "auth_password"
	KeyEnableGzip = "enable_gzip"
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
	KeylocalTimezone = "Local"
	KeyUTCTimezone   = "UTC"
	KeyPRCTimezone   = "PRC"
)

const KeySendTime = "sendTime"
const KeyTimestamp = "@timestamp"

// NewElasticSender New ElasticSender
func NewElasticSender(conf config.MapConf) (sender Sender, err error) {
	host, err := conf.GetStringList(KeyElasticHost)
	if err != nil {
		return
	}
	for i, h := range host {
		if !strings.HasPrefix(h, "http://") {
			host[i] = fmt.Sprintf("http://%s", h)
		}
	}

	index, err := conf.GetString(KeyElasticIndex)
	if err != nil {
		return
	}

	// 索引后缀模式
	indexStrategy, _ := conf.GetStringOr(KeyElasticIndexStrategy, KeyDefaultIndexStrategy)
	timezone, _ := conf.GetStringOr(KeyElasticTimezone, KeyUTCTimezone)
	timeZone, err := time.LoadLocation(timezone)
	if err != nil {
		return
	}
	logkitSendTime, _ := conf.GetBoolOr(KeyLogkitSendTime, true)
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))
	eVersion, _ := conf.GetStringOr(KeyElasticVersion, ElasticVersion3)

	strategy := []string{KeyDefaultIndexStrategy, KeyYearIndexStrategy, KeyMonthIndexStrategy, KeyDayIndexStrategy}

	i, err := machPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	authUsername, _ := conf.GetStringOr(KeyAuthUsername, config.BLOGIC_ES_AUTH_NAME)
	authPassword, _ :=conf.GetPasswordEnvStringOr(KeyAuthPassword, config.BLOGIC_ES_AUTH_PASSWORD)
	enableGzip, _ := conf.GetBoolOr(KeyEnableGzip, false)

	// 初始化 client
	var elasticV3Client *elasticV3.Client
	var elasticV5Client *elasticV5.Client
	var elasticV6Client *elasticV6.Client
	switch eVersion {
	case ElasticVersion6:
		optFns := []elasticV6.ClientOptionFunc{
			elasticV6.SetSniff(false),
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(host...),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV6.SetBasicAuth(authUsername, authPassword))
		}

		elasticV6Client, err = elasticV6.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	case ElasticVersion3:
		optFns := []elasticV3.ClientOptionFunc{
			elasticV3.SetSniff(false),
			elasticV3.SetHealthcheck(false),
			elasticV3.SetURL(host...),
			elasticV3.SetGzip(enableGzip),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV3.SetBasicAuth(authUsername, authPassword))
		}

		elasticV3Client, err = elasticV3.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	default:
		httpClient := &http.Client{
			Timeout: 300 * time.Second,
		}
		optFns := []elasticV5.ClientOptionFunc{
			elasticV5.SetSniff(false),
			elasticV5.SetHealthcheck(false),
			elasticV5.SetURL(host...),
			elasticV5.SetGzip(enableGzip),
			elasticV5.SetHttpClient(httpClient),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV5.SetBasicAuth(authUsername, authPassword))
		}

		elasticV5Client, err = elasticV5.NewClient(optFns...)
		if err != nil {
			return nil, err
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
	err = fmt.Errorf("Unknown index_strategy: '%s'", s)
	return i, err
}

// Name ElasticSearchSenderName
func (ess *ElasticsearchSender) Name() string {
	return "//" + ess.indexName
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
			//字段名称替换
			if makeDoc {
				doc = ess.wrapDoc(doc)
			}
			//添加额外字段
			addExtraField(ess, doc)
			//计算索引
			indexName = buildIndexName(ess.indexName, ess.timeZone, ess.intervalIndex, doc)

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
			//字段名称替换
			if makeDoc {
				doc = ess.wrapDoc(doc)
			}
			//添加额外字段
			addExtraField(ess, doc)
			//计算索引
			indexName = buildIndexName(ess.indexName, ess.timeZone, ess.intervalIndex, doc)

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
			//字段名称替换
			if makeDoc {
				doc = ess.wrapDoc(doc)
			}
			//添加额外字段
			addExtraField(ess, doc)
			//计算索引
			indexName = buildIndexName(ess.indexName, ess.timeZone, ess.intervalIndex, doc)

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

func addExtraField(ess *ElasticsearchSender, doc Data) {
	now := time.Now()
	//添加发送时间
	if ess.logkitSendTime {
		doc[KeySendTime] = now.In(ess.timeZone)
	}

	if _, exist := doc[KeyTimestamp]; !exist {
		doc[KeyTimestamp] = now.Format(time.RFC3339Nano)
	}

	//如果不存在KeyHostName字段,默认添加
	if _, ok := doc[KeyHostName]; !ok {
		hostName, oErr := os.Hostname()
		if oErr != nil {
			doc[KeyHostName] = "unKnown"
		} else {
			doc[KeyHostName] = hostName
		}
	}
}

func buildIndexName(indexName string, timeZone *time.Location, size int, doc Data) string {
	var t time.Time
	if timestampStr, exist := doc[KeyTimestamp]; exist {
		if timestampStr, ok := timestampStr.(string); ok {
			if timestamp, err := times.StrToTime(timestampStr); err == nil {
				t = timestamp.In(timeZone)
			} else {
				t = time.Now().In(timeZone)
			}
		} else {
			t = time.Now().In(timeZone)
		}
	} else {
		t = time.Now().In(timeZone)
	}
	intervals := []string{strconv.Itoa(t.Year()), strconv.Itoa(int(t.Month())), strconv.Itoa(t.Day())}
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
	return indexName
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
