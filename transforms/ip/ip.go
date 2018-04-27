package ip

import (
	"errors"
	"fmt"

	"github.com/oschwald/geoip2-golang"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
	"net"
)

type IpTransformer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	DataPath  string `json:"data_path"`
	db        *geoip2.Reader
	stats     StatsInfo
}

func (it *IpTransformer) Init() error {
	db, err := geoip2.Open(it.DataPath)
	if err != nil {
		return err
	}
	it.db = db
	return nil
}

func (it *IpTransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("IP transformer not support rawTransform")
}

func (it *IpTransformer) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	keys := GetKeys(it.Key)
	newkeys := make([]string, len(keys))
	for i := range datas {
		copy(newkeys, keys)
		val, gerr := GetMapValue(datas[i], keys...)
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

		record, nerr := it.db.City(net.ParseIP(strval))
		if nerr != nil {
			err = nerr
			errnums++
			continue
		}
		newkeys[len(newkeys)-1] = "City"
		SetMapValue(datas[i], record.City.Names["zh-CN"], false, newkeys...)
		Subdivisions := ""
		if len(record.Subdivisions) > 0 {
			Subdivisions = record.Subdivisions[0].Names["zh-CN"]
		}
		newkeys[len(newkeys)-1] = "Region"
		SetMapValue(datas[i], Subdivisions, false, newkeys...)
		newkeys[len(newkeys)-1] = "Country"
		SetMapValue(datas[i], record.Country.Names["zh-CN"], false, newkeys...)
		newkeys[len(newkeys)-1] = "Latitude"
		SetMapValue(datas[i], record.Location.Latitude, false, newkeys...)
		newkeys[len(newkeys)-1] = "Longitude"
		SetMapValue(datas[i], record.Location.Longitude, false, newkeys...)
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
		transforms.KeyFieldName,
		{
			KeyName:      "data_path",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "your/path/to/ip.dat",
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

func (it *IpTransformer) Stats() StatsInfo {
	return it.stats
}

func init() {
	transforms.Add("IP", func() transforms.Transformer {
		return &IpTransformer{}
	})
}
