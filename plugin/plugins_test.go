package plugin

import "testing"

func TestListPlugins(t *testing.T) {
	config = &PluginConfig{
		Enabled:true,
		Dir:"E:\\Go\\GOPATH\\src\\github.com\\qiniu\\logkit\\plugins",
		Git:"",
	}
	ListPlugins()
}

func TestPluginRun(t *testing.T) {
	plugin := &Plugin{
		FilePath:"E:\\Go\\GOPATH\\src\\github.com\\qiniu\\logkit\\plugins\\hello",
		MTime:1000000,
		Cycle:10,
		ExecFile:"10_hello.exe",
	}
	PluginRun(plugin)
}