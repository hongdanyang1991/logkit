package ip

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/wangtuanjie/ip17mon"
	"strconv"
	"math/rand"
	"time"
	"strings"
)

//更全的免费数据可以在ipip.net下载
type IpTransformer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	DataPath  string `json:"data_path"`
	loc       *ip17mon.Locator
	stats     utils.StatsInfo
}

func (it *IpTransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("IP transformer not support rawTransform")
}

var ipList = [][]string{
	{"58", "50", "", ""},
	{"58", "60", "", ""},
	{"58", "33", "", ""},
	{"59", "155", "", ""},
	{"60", "247", "", ""},
	{"116", "1", "", ""},
	{"116", "2", "", ""},
	{"116", "3", "", ""},
	{"116", "4", "", ""},
	{"116", "5", "", ""},
	{"116", "6", "", ""},
	{"116", "7", "", ""},
	{"116", "8", "", ""},
	{"121", "4", "", ""},
	{"121", "5", "", ""},
	{"121", "8", "", ""},
	{"121", "9", "", ""},
	{"121", "10", "", ""},
	{"121", "11", "", ""},
	{"121", "12", "", ""},
	{"121", "13", "", ""},
	{"121", "14", "", ""},
	{"121", "15", "", ""},
	{"121", "16", "", ""},
	{"121", "59", "", ""},
	{"121", "62", "", ""},
	{"121", "68", "", ""},
	{"122", "4", "", ""},
	{"122", "51", "", ""},
	{"123", "4", "", ""},
	{"110", "96", "", ""},
	{"218", "246", "", ""},
	{"121", "89", "", ""},
	{"116", "85", "", ""},
	{"211", "81", "", ""},
	{"124", "192", "", ""},
	{"118", "190", "", ""},
	{"211", "100", "", ""},
	{"124", "74", "", ""},
	{"124", "75", "", ""},
	{"218", "1", "", ""},
	{"61", "152", "", ""},
	{"61", "170", "", ""},
	{"116", "246", "", ""},
}

//映射ip字段
func (it *IpTransformer) randomIp(doc Data) {
	rawIp := doc[it.Key]
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	len := len(ipList)
	seed := r.Intn(len - 1)
	ip := ipList[seed]
	ip[2] = strconv.Itoa(r.Intn(255))
	ip[3] = strconv.Itoa(r.Intn(255))
	var ipStr = strings.Join(ip, ".")
	doc[it.Key] = ipStr
	message := doc["message"]
	if messageStr, ok := message.(string); ok {
		if rawIpStr, ok := rawIp.(string); ok {
			message = strings.Replace(messageStr, rawIpStr, ipStr, 1)
			doc["message"] = message
		}
	}
}

func (it *IpTransformer) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	if it.loc == nil {
		it.loc, err = ip17mon.NewLocator(it.DataPath)
		if err != nil {
			return datas, err
		}
	}
	errnums := 0
	keys := utils.GetKeys(it.Key)
	newkeys := make([]string, len(keys))
	for i := range datas {
		//工业互联网 内置
		if _, ok := datas[i][it.Key]; ok {
			it.randomIp(datas[i])
		}


		copy(newkeys, keys)
		val, gerr := utils.GetMapValue(datas[i], keys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", it.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", it.Key)
			continue
		}
		info, nerr := it.loc.Find(strval)
		if nerr != nil {
			err = nerr
			errnums++
			continue
		}
		newkeys[len(newkeys)-1] = "Region"
		utils.SetMapValue(datas[i], info.Region, false, newkeys...)
		newkeys[len(newkeys)-1] = "City"
		utils.SetMapValue(datas[i], info.City, false, newkeys...)
		newkeys[len(newkeys)-1] = "Country"
		utils.SetMapValue(datas[i], info.Country, false, newkeys...)
		newkeys[len(newkeys)-1] = "Isp"
		utils.SetMapValue(datas[i], info.Isp, false, newkeys...)
	}
	if err != nil {
		it.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform IP, last error info is %v", errnums, err)
	}
	it.stats.Errors += int64(errnums)
	it.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (it *IpTransformer) Description() string {
	//return "transform ip to country region and isp"
	return "获取IP的区域、国家、城市和运营商信息"
}

func (it *IpTransformer) Type() string {
	return "IP"
}

func (it *IpTransformer) SampleConfig() string {
	return `{
		"type":"IP",
		"stage":"after_parser",
		"key":"MyIpFieldKey",
		"data_path":"your/path/to/ip.dat"
	}`
}

func (it *IpTransformer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
		{
			KeyName:      "data_path",
			ChooseOnly:   false,
			Default:      "your/path/to/ip.dat",
			DefaultNoUse: true,
			Description:  "IP数据库路径(data_path)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (it *IpTransformer) Stage() string {
	if it.StageTime == "" {
		return transforms.StageAfterParser
	}
	return it.StageTime
}

func (it *IpTransformer) Stats() utils.StatsInfo {
	return it.stats
}

func init() {
	transforms.Add("IP", func() transforms.Transformer {
		return &IpTransformer{}
	})
}
