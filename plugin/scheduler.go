package plugin

import (
	"bytes"
	"github.com/toolkits/file"
	"github.com/toolkits/sys"
	"log"
	"os/exec"
	"path/filepath"
	//"syscall"
	"time"
	"encoding/json"
)

type PluginScheduler struct {
	Ticker *time.Ticker
	Plugin *Plugin
	Quit   chan struct{}
}

func NewPluginScheduler(p *Plugin) *PluginScheduler {
	scheduler := PluginScheduler{Plugin: p}
	scheduler.Ticker = time.NewTicker(time.Duration(p.Cycle) * time.Second)
	scheduler.Quit = make(chan struct{})
	return &scheduler
}

func (this *PluginScheduler) Schedule() {
	go func() {
		for {
			select {
			case <-this.Ticker.C:
				PluginRun(this.Plugin)
			case <-this.Quit:
				this.Ticker.Stop()
				return
			}
		}
	}()
}

func (this *PluginScheduler) Stop() {
	close(this.Quit)
}

func PluginRun(plugin *Plugin) (metric map[string]interface{}) {

	timeout := plugin.Cycle*1000 - 500
	exePath := filepath.Join(plugin.FilePath, plugin.ExecFile)

	if !file.IsExist(exePath) {
		log.Println("no such plugin:", exePath)
		return
	}

	log.Println(exePath, "running...")

	cmd := exec.Command(exePath)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	//linux环境下放开-------------------------
	//cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Start()
	log.Println("plugin started:", exePath)

	err, isTimeout := sys.CmdRunWithTimeout(cmd, time.Duration(timeout)*time.Millisecond)

	errStr := stderr.String()
	if errStr != "" {
		logFile := filepath.Join(plugin.FilePath, "log", "stderr.log")
		if _, err = file.WriteString(logFile, errStr); err != nil {
			log.Printf("[ERROR] write log to %s fail, error: %s\n", logFile, err)
		}
	}

	if isTimeout {
		// has be killed
		if err == nil  {
			log.Println("[INFO] timeout and kill process", exePath, "successfully")
		}

		if err != nil {
			log.Println("[ERROR] kill process", exePath, "occur error:", err)
		}

		return
	}

	if err != nil {
		log.Println("[ERROR] exec plugin", exePath, "fail. error:", err)
		return
	}

	// exec successfully
	data := stdout.Bytes()
	if len(data) == 0 {
		log.Println("[DEBUG] stdout of", exePath, "is blank")
		return
	}
	err = json.Unmarshal(data, &metric)
	if err != nil {
		log.Printf("[ERROR] json.Unmarshal stdout of %s fail. error:%s stdout: \n%s\n", exePath, err, stdout.String())
		return
	}
	return
	//g.SendToTransfer(metrics)
}
