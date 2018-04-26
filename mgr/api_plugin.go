package mgr

import (
	"github.com/labstack/echo"
	"github.com/qiniu/logkit/plugin"
)

//GET  /logkit/plugin/sync
func (rs *RestService) SyncPlugins() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, plugin.SyncPlugins())
	}
}

//GET  /logkit/plugin/list
func (rs *RestService) ListPlugins() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, plugin.ListPlugins())
	}
}
