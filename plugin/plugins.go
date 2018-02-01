package plugin

import (
	"path/filepath"
	"io/ioutil"
	"strings"
	"strconv"
	"github.com/toolkits/file"
	"log"

	"sync"
	"os"
)

type Plugin struct {
	FilePath string    //插件路径
	MTime    int64	   //修改时间
	Cycle    int	   //运行周期
	ExecFile string    //可执行文件名称
	ConfFile string    //配置文件名称
}

type PluginConfig struct {
	Enabled bool   `json:"enabled"`		//是否禁用plugin
	Dir     string `json:"dir"`			//plugin路径
	Git     string `json:"git"`			//plugin git地址
	//LogDir  string `json:"logs"`		//plugin日志输出路径
}

var (
	Plugins              = make(map[string]*Plugin)
	PluginsWithScheduler = make(map[string]*PluginScheduler)
	config     			 *PluginConfig
	lock       			 = new(sync.RWMutex)
	confFile             = "conf"
	batchCount           = 10
)

func Config() *PluginConfig {
	lock.RLock()
	defer lock.RUnlock()
	return config
}


//列出./plugins目录下的所有插件
func ListPlugins() map[string]*Plugin {
	ret := make(map[string]*Plugin)
	pConfig := Config()
	dir := pConfig.Dir

	if !file.IsExist(dir) || file.IsFile(dir) {
		return ret
	}

	if !pConfig.Enabled {
		return ret
	}
	//plugins路径
	pluginFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("can not list files under", dir)
		return ret
	}

	for _, pluginFile := range pluginFiles {
		if !pluginFile.IsDir() {
			continue
		}

		filename := pluginFile.Name()

		pluginPath := filepath.Join(dir, filename)
		fs, err := ioutil.ReadDir(pluginPath)
		if err != nil {
			log.Println("can not list files under", pluginPath)
			return ret
		}
		for _, f := range fs {
			confExist := false
			if f.IsDir() {
				if f.Name() != confFile {
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
				confFile := filepath.Join(pluginPath, confFile)
				os.Mkdir(confFile, os.ModePerm)
			}
			plugin := &Plugin{FilePath: pluginPath, MTime: f.ModTime().Unix(), Cycle: cycle, ExecFile:fName, ConfFile:confFile}
			ret[pluginPath] = plugin
			break
		}
	}
	return ret
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
	for fpath, newPlugin := range newPlugins {
		if _, ok := Plugins[fpath]; ok && newPlugin.MTime == Plugins[fpath].MTime {
			continue
		}

		Plugins[fpath] = newPlugin
		sch := NewPluginScheduler(newPlugin)
		PluginsWithScheduler[fpath] = sch
		sch.Schedule()
	}
}

func ClearAllPlugins() {
	for k := range Plugins {
		deletePlugin(k)
	}
}

func deletePlugin(key string) {
	v, ok := PluginsWithScheduler[key]
	if ok {
		v.Stop()
		delete(PluginsWithScheduler, key)
	}
	delete(Plugins, key)
}
