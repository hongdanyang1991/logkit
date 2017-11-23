package mgr

import (
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	"fmt"
	"encoding/json"
	"github.com/qiniu/logkit/sender"
)

// GET /logkit/transformer/usages
func (rs *RestService) GetTransformerUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		var ModeUsages []utils.KeyValue
		for _, v := range transforms.Transformers {
			cr := v()
			ModeUsages = append(ModeUsages, utils.KeyValue{
				Key:   cr.Type(),
				Value: cr.Description(),
			})
		}
		return c.JSON(http.StatusOK, ModeUsages)
	}
}

//GET /logkit/transformer/options
func (rs *RestService) GetTransformerOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		ModeKeyOptions := make(map[string][]utils.Option)
		for _, v := range transforms.Transformers {
			cr := v()
			ModeKeyOptions[cr.Type()] = cr.ConfigOptions()
		}
		return c.JSON(http.StatusOK, ModeKeyOptions)
	}
}

//GET /logkit/transformer/sampleconfigs
func (rs *RestService) GetTransformerSampleConfigs() echo.HandlerFunc {
	return func(c echo.Context) error {
		SampleConfigs := make(map[string]string)
		for _, v := range transforms.Transformers {
			cr := v()
			SampleConfigs[cr.Type()] = cr.SampleConfig()
		}
		return c.JSON(http.StatusOK, SampleConfigs)
	}
}

// POST /logkit/transformer/transform
// Transform (multiple logs/single log) in (json array/json object) format with registered transformers
// Return result string in json array format
func (rs *RestService) PostTransform() echo.HandlerFunc {
	return func(c echo.Context) error {
		var ok bool                      // value exists in a map / assertion is successful
		var jsonErr error                // error caused by json marshal or unmarshal
		var transErr error               // error caused by incorrect transform process
		var tp string                    // transformer type string
		var trans transforms.Transformer // transformer itself
		var rawLogs string               // sample logs picked from request in json format
		var data = []sender.Data{}       // multiple sample logs in map format
		var singleData sender.Data       // single sample log in map format
		var bts []byte
		var reqConf map[string]interface{} // request body params in map format

		// bind request context onto map[string]string
		if err := c.Bind(&reqConf); err != nil {
			return err
		}

		// Get params from request & Valid Params & Initialize transformer using valid params
		// param 1: transformer type
		if _, ok = reqConf[transforms.KeyType]; !ok {
			// param absence
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("missing param %s", transforms.KeyType))
		}
		tp, ok = (reqConf[transforms.KeyType]).(string)
		if !ok {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("param %s must be of type string", transforms.KeyType))
		}
		create, ok := transforms.Transformers[tp]
		if !ok {
			// no such type transformer
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("transformer of type %v not exist", tp))
		}
		// param 2: sample logs
		if _, ok = reqConf[KeySampleLog]; !ok {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("missing param %s", KeySampleLog))
		}
		if rawLogs, ok = (reqConf[KeySampleLog]).(string); !ok {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("param %s must be of type string", KeySampleLog))
		}
		if jsonErr = json.Unmarshal([]byte(rawLogs), &singleData); jsonErr != nil {
			// may be multiple sample logs
			if jsonErr = json.Unmarshal([]byte(rawLogs), &data); jsonErr != nil {
				// invalid JSON, neither multiple sample logs nor single sample log
				return echo.NewHTTPError(http.StatusBadRequest, jsonErr.Error())
			}
		} else {
			// is single log, and method transformer.transform(data []sender.Data) accept a param of slice type
			data = append(data, singleData)
		}
		// initialize transformer
		trans = create()
		reqConf = convertWebTransformerConfig(reqConf)
		delete(reqConf, KeySampleLog)
		if bts, jsonErr = json.Marshal(reqConf); jsonErr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, jsonErr.Error())
		}
		if jsonErr = json.Unmarshal(bts, trans); jsonErr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, jsonErr.Error())
		}
		if trans, ok := trans.(transforms.Initialize); ok {
			if err := trans.Init(); err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, err.Error())
			}
		}

		// Act Transform
		data, transErr = trans.Transform(data)
		if transErr != nil {
			se, ok := transErr.(*utils.StatsError)
			if ok {
				transErr = se.ErrorDetail
			}
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("transform processing error %v", transErr))
		}

		// Transform Success
		return c.JSON(http.StatusOK, data)
	}
}