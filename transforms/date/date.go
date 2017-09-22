package date

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type DateTrans struct {
	Key          string `json:"key"`
	Offset       int    `json:"offset"`
	LayoutBefore string `json:"time_layout_before"`
	LayoutAfter  string `json:"time_layout_after"`
	stats        utils.StatsInfo
}

func (g *DateTrans) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("date transformer not support rawTransform")
}

func (g *DateTrans) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	for i := range datas {
		val, ok := datas[i][g.Key]
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		val, err = g.convertDate(val)
		if err != nil {
			errnums++
			continue
		}
		datas[i][g.Key] = val
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform replace, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *DateTrans) convertDate(v interface{}) (interface{}, error) {
	var s int64
	switch newv := v.(type) {
	case int64:
		s = newv
	case int:
		s = int64(newv)
	case int32:
		s = int64(newv)
	case int16:
		s = int64(newv)
	case uint64:
		s = int64(newv)
	case uint32:
		s = int64(newv)
	case string:
		if g.LayoutBefore != "" {
			t, err := time.Parse(g.LayoutBefore, newv)
			if err != nil {
				return v, fmt.Errorf("can not parse %v with layout %v", newv, g.LayoutBefore)
			}
			return g.formatWithUserOption(t), nil
		}
		t, err := times.StrToTime(newv)
		if err != nil {
			return v, err
		}
		return g.formatWithUserOption(t), nil
	case json.Number:
		jsonNumber, err := newv.Int64()
		if err != nil {
			return v, err
		}
		s = jsonNumber
	default:
		return v, fmt.Errorf("can not parse %v type %v as date time", v, reflect.TypeOf(v))
	}
	news := s
	timestamp := strconv.FormatInt(news, 10)
	timeSecondPrecision := 16
	//补齐16位
	for i := len(timestamp); i < timeSecondPrecision; i++ {
		timestamp += "0"
	}
	// 取前16位，截取精度 微妙
	timestamp = timestamp[0:timeSecondPrecision]
	t, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return v, err
	}
	tm := time.Unix(0, t*int64(time.Microsecond))
	return g.formatWithUserOption(tm), nil
}

func (g *DateTrans) formatWithUserOption(t time.Time) interface{} {
	t = t.Add(time.Duration(g.Offset) * time.Hour)
	if g.LayoutAfter != "" {
		return t.Format(g.LayoutAfter)
	}
	return t.Format(time.RFC3339Nano)
}

func (g *DateTrans) Description() string {
	return "transform date can transform string/long to specified date format"
}

func (g *DateTrans) Type() string {
	return "date"
}

func (g *DateTrans) SampleConfig() string {
	return `{
		"type":"date",
		"key":"DateFieldKey",
		"offset":0,
		"time_layout_before":"",
		"time_layout_after":"2006-01-02T15:04:05Z07:00"
	}`
}

func (g *DateTrans) Stage() string {
	return transforms.StageAfterParser
}

func (g *DateTrans) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("date", func() transforms.Transformer {
		return &DateTrans{}
	})
}
