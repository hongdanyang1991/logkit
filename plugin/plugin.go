package plugin

import (
	"path/filepath"
	"io/ioutil"
	"strings"
	"strconv"
	"github.com/toolkits/file"
	"github.com/qiniu/log"

	"sync"
	"os"

	"bytes"
	"github.com/toolkits/sys"
	"os/exec"
	//"syscall"
	"time"
	"encoding/json"
	"fmt"
)

type Plugin struct {
	Type 		    string
	Path            string	  //插件路径
	MTime    		int64	  //修改时间
	DefaultCycle    int	   	  //默认运行周期
	ExecFile 		string    //可执行文件名称
	ConfDir 		string    //配置文件名称
}

type Config struct {
	Enabled bool   `json:"enabled"`		//是否禁用plugin
	Dir     string `json:"dir"`			//plugin路径
	Git     string `json:"git"`			//plugin git地址
	//LogDir  string `json:"logs"`		//plugin日志输出路径
}

var (
	Plugins              = make(map[string]*Plugin)
	Conf     			 *Config
	lock       			 = new(sync.RWMutex)
	confDir              = "conf"
)


func ListPlugins() map[string]*Plugin {
	return Plugins
}


//同步插件
func SyncPlugins() error {
	plugins := make(map[string]*Plugin)
	if !Conf.Enabled {
		return fmt.Errorf("plugin is not allowed")
	}
	dir := Conf.Dir

	if !file.IsExist(dir) || file.IsFile(dir) {
		return fmt.Errorf("the file %v is not exist", dir)
	}


	//plugins路径
	pluginFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("can not list files under", dir)
		return fmt.Errorf("can not list files under %v", dir)
	}

	for _, pluginFile := range pluginFiles {
		if !pluginFile.IsDir() {
			continue
		}

		fileName := pluginFile.Name()

		pluginPath := filepath.Join(dir, fileName)
		fs, err := ioutil.ReadDir(pluginPath)
		if err != nil {
			log.Println("can not list files under", pluginPath)
			return fmt.Errorf("can not list files under %v", pluginPath)
		}
		for _, f := range fs {
			confExist := false
			if f.IsDir() {
				if f.Name() != confDir {
					continue
				}
				confExist = true
			}
			fName := f.Name()
			arr := strings.Split(fName, "_")
			if len(arr) < 2 {
				continue
			}

			// filename should be: $cycle_$xx
			var cycle int
			cycle, err = strconv.Atoi(arr[0])
			if err != nil {
				continue
			}
			if !confExist {
				confFile := filepath.Join(pluginPath, confDir)
				os.Mkdir(confFile, os.ModePerm)
			}
			plugin := &Plugin{Type: fileName,Path: pluginPath, MTime: f.ModTime().Unix(), DefaultCycle: cycle, ExecFile:fName, ConfDir:confDir}
			plugins[fileName] = plugin
			break
		}
	}
	Plugins = plugins
	return nil
}

func PluginRun(plugin *Plugin, configFile string, Cycle int) (metric map[string]interface{}, err error) {

	timeout := Cycle * 1000 - 500
	exePath := filepath.Join(plugin.Path, plugin.ExecFile)

	if !file.IsExist(exePath) {
		return nil, fmt.Errorf("no executable file error :%v", exePath)
	}

	log.Debugf(exePath, " running...")

	cmd := exec.Command(exePath, "-f", configFile)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	//linux环境下放开-------------------------
	//cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Start()
	log.Debugf("plugin started: ", exePath)

	err, isTimeout := sys.CmdRunWithTimeout(cmd, time.Duration(timeout)*time.Millisecond)

	if isTimeout {
		// has be killed
		if err == nil  {
			err = fmt.Errorf("plugin %v run timeout and has been killed successfully", exePath)
		}

		if err != nil {
			err = fmt.Errorf("plugin %v run timeout with err %v and has been killed successfully", exePath, err.Error())
		}
		return
	}

	if err != nil {
		err = fmt.Errorf("plugin %v run failed with err %v and has been killed successfully", exePath, err.Error())
		return
	}

	// exec successfully
	data := stdout.Bytes()
	if len(data) == 0 {
		err = fmt.Errorf("plugin %v stdout is blank", exePath)
		return
	}
	err = json.Unmarshal(data, &metric)
	if err != nil {
		err = fmt.Errorf("json.Unmarshal stdout of %s fail. error:%s stdout: \n%s\n", exePath, err, stdout.String())
		return nil , err
	}
	return
}

func DelNoUsePlugins(newPlugins map[string]*Plugin) {
	for currKey, currPlugin := range Plugins {
		newPlugin, ok := newPlugins[currKey]
		if !ok || currPlugin.MTime != newPlugin.MTime {
			deletePlugin(currKey)
		}
	}
}

func AddNewPlugins(newPlugins map[string]*Plugin) {

}

func ClearAllPlugins() {
	for k := range Plugins {
		deletePlugin(k)
	}
}

func deletePlugin(key string) {

}
