package mgr

import (
	"github.com/qiniu/logkit/plugin"
	"github.com/qiniu/logkit/transforms"
	"time"
	"sync"
	"github.com/qiniu/logkit/sender"
)


type PluginConfig struct {
	PluginType string
	Cycle	   int						`json:"cycle"`
	BatchCount int  					`json:"batchCount"`
	Config     map[string]interface{} 	`json:"config,omitempty"`
}

type PluginRunner struct {
	RunnerName 		string					`json:"name"`
	PluginScheduler *plugin.PluginScheduler
	transformers	[]transforms.Transformer
	collectInterval time.Duration
	rs              RunnerStatus
	lastRs          RunnerStatus
	rsMutex         *sync.RWMutex
	lastSend        time.Time
	stopped         int32
	exitChan        chan struct{}
}

func NewPluginRunner(rc RunnerConfig, transformers []transforms.Transformer, senders []sender.Sender) (runner *PluginRunner, err error) {


	return
}