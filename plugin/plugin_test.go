package plugin

import (
	"fmt"
	"testing"
)

func TestListPlugins(t *testing.T) {
	Conf = &Config{
		Enabled: true,
		Dir:     "E:\\Go\\GOPATH\\src\\github.com\\qiniu\\logkit\\plugins",
		Git:     "",
	}
	SyncPlugins()
}

func TestPluginRun(t *testing.T) {
	plugin := &Plugin{
		Path:         "E:\\Go\\GOPATH\\src\\github.com\\qiniu\\logkit\\plugins\\hello",
		MTime:        1000000,
		DefaultCycle: 10,
		ExecFile:     "10_hello.exe",
	}
	metric := PluginRun(plugin, "a")
	fmt.Println(metric)
}
