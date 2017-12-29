package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/cli"
	config "github.com/qiniu/logkit/conf"
	_ "github.com/qiniu/logkit/metric/all"
	"github.com/qiniu/logkit/mgr"
	"github.com/qiniu/logkit/times"
	_ "github.com/qiniu/logkit/transforms/all"
	"github.com/qiniu/logkit/utils"

	"github.com/labstack/echo"
	"net/url"
	"io/ioutil"
	"net"
	"strings"
)

//Config of logkit
type Config struct {
	BindIP           string   `json:"bind_ip"`
	BindPort         string   `json:"bind_port"`
	Tenant           string   `json:"tenant"`
	BlogicUrl        string   `json:"blogic_url"`
	MaxProcs         int      `json:"max_procs"`
	DebugLevel       int      `json:"debug_level"`
	ProfileHost      string   `json:"profile_host"`
	ConfsPath        []string `json:"confs_path"`
	LogPath          string   `json:"log"`
	CleanSelfLog     bool     `json:"clean_self_log"`
	CleanSelfDir     string   `json:"clean_self_dir"`
	CleanSelfPattern string   `json:"clean_self_pattern"`
	TimeLayouts      []string `json:"timeformat_layouts"`
	CleanSelfLogCnt  int      `json:"clean_self_cnt"`
	StaticRootPath   string   `json:"static_root_path"`
	mgr.ManagerConfig
}

var DEFAULT_PORT = "8100"

var conf Config

const (
	NextVersion       = "v1.4.2"
	defaultReserveCnt = 5
	defaultLogDir     = "./run"
	defaultLogPattern = "*.log-*"
	defaultRotateSize = 100 * 1024 * 1024
)

const usage = `logkit, Very easy-to-use server agent for collecting & sending logs & metrics.

Usage:

  logkit [commands|flags]

The commands & flags are:

  -v                 print the version to stdout.
  -h                 print logkit usage info to stdout.
  -upgrade           check and upgrade version.

  -f <file>          configuration file to load

Examples:

  # start logkit
  logkit -f logkit.conf

  # check version
  logkit -v

  # checking and upgrade version
  logkit -upgrade
`

var (
	fversion = flag.Bool("v", false, "print the version to stdout")
	upgrade  = flag.Bool("upgrade", false, "check and upgrade version")
	confName = flag.String("f", "logkit.conf", "configuration file to load")
)

func getValidPath(confPaths []string) (paths []string) {
	paths = make([]string, 0)
	exits := make(map[string]bool)
	for _, v := range confPaths {
		rp, err := filepath.Abs(v)
		if err != nil {
			log.Errorf("Get real path of ConfsPath %v error %v, ignore it", v, rp)
			continue
		}
		if _, ok := exits[rp]; ok {
			log.Errorf("ConfsPath %v duplicated, ignore", rp)
			continue
		}
		exits[rp] = true
		paths = append(paths, rp)
	}
	return
}

type MatchFile struct {
	Name    string
	ModTime time.Time
}

type MatchFiles []MatchFile

func (f MatchFiles) Len() int           { return len(f) }
func (f MatchFiles) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f MatchFiles) Less(i, j int) bool { return f[i].ModTime.Before(f[j].ModTime) }

func cleanLogkitLog(dir, pattern string, reserveCnt int) {
	var err error
	path := filepath.Join(dir, pattern)
	matches, err := filepath.Glob(path)
	if err != nil {
		log.Errorf("filepath.Glob path %v error %v", path, err)
		return
	}
	var files MatchFiles
	for _, name := range matches {
		info, err := os.Stat(name)
		if err != nil {
			log.Errorf("os.Stat name %v error %v", name, err)
			continue
		}
		files = append(files, MatchFile{
			Name:    name,
			ModTime: info.ModTime(),
		})
	}
	if len(files) <= reserveCnt {
		return
	}
	sort.Sort(files)
	for _, f := range files[0 : len(files)-reserveCnt] {
		err := os.Remove(f.Name)
		if err != nil {
			log.Errorf("Remove %s failed , error: %v", f, err)
			continue
		}
	}
	return
}

func loopCleanLogkitLog(dir, pattern string, reserveCnt int, dur time.Duration, exitchan chan struct{}) {
	if len(dir) <= 0 {
		dir = defaultLogDir
	}
	if len(pattern) <= 0 {
		pattern = defaultLogPattern
	}
	if reserveCnt <= 0 {
		reserveCnt = defaultReserveCnt
	}
	ticker := time.NewTicker(dur)
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			cleanLogkitLog(dir, pattern, reserveCnt)
		}
	}
}

func rotateLog(path string) (file *os.File, err error) {
	newfile := path + "-" + time.Now().Format("0102030405")
	file, err = os.OpenFile(newfile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		err = fmt.Errorf("rotateLog open newfile %v err %v", newfile, err)
		return
	}
	log.SetOutput(file)
	return
}

func loopRotateLogs(path string, rotateSize int64, dur time.Duration, exitchan chan struct{}) {
	file, err := rotateLog(path)
	if err != nil {
		log.Fatal(err)
	}
	ticker := time.NewTicker(dur)
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			info, err := file.Stat()
			if err != nil {
				log.Warnf("stat log error %v", err)
			} else {
				if info.Size() >= rotateSize {
					newfile, err := rotateLog(path)
					if err != nil {
						log.Errorf("rotate log %v error %v, use old log to write logkit log", path, err)
					} else {
						file.Close()
						file = newfile
					}
				}
			}

		}
	}
}

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}

func sendBlogic(tenant string,bindIP string,bindPort string){
	blogicUrl := conf.BlogicUrl
	if blogicUrl ==""{
		log.Errorf("获取blogic访问地址失败,无法将服务自动注册到blogic中")
		return
	}
	if tenant == "" {
		tenant = "admin"
		log.Infof("获取配置文件无效tenant项，使用默认租户%v",tenant)
	}
	if bindIP ==""{
		addrs, _ := net.InterfaceAddrs()
		for _, address := range addrs {
			// 检查ip地址判断是否回环地址
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback(){
				if ipnet.IP.To4()!= nil {
					bindIP = ipnet.IP.String()
				}
			}
		}
		log.Infof("获取配置文件无效bind_ip项，使用默认本机IP:%v",bindIP)
	}
	if bindPort ==""{
		bindPort = DEFAULT_PORT
		log.Infof("获取配置文件无效bind_port项，使用默认%v端口",bindPort)
	}
	registerUrl := fmt.Sprintf("http://%v/data/logkit/collector/register", blogicUrl)
	//网络请求可以多重试 避免一次请求出错
	response, err := http.PostForm(registerUrl,url.Values{"ip": {bindIP}, "port": {bindPort},"tenant": {tenant}})

	if err!=nil{
		log.Errorf("注册请求发送失败，错误信息：%v",err)
	}else{
		//请求完了关闭回复主体
		defer response.Body.Close()
		body,_ := ioutil.ReadAll(response.Body)
		log.Infof("tenant：%v；bindIP：%v；bindPort：%v",tenant,bindIP,bindPort)
		log.Infof("注册请求发送成功，返回信息：%v",string(body))
	}
}

//！！！注意： 自动生成 grok pattern代码，下述注释请勿删除！！！
//go:generate go run generators/grok_pattern_generater.go
func main() {

	flag.Usage = func() { usageExit(0) }
	flag.Parse()
	switch {
	case *fversion:
		fmt.Println("logkit version: ", NextVersion)
		osInfo := utils.GetOSInfo()
		fmt.Println("Hostname: ", osInfo.Hostname)
		fmt.Println("Core: ", osInfo.Core)
		fmt.Println("OS: ", osInfo.OS)
		fmt.Println("Platform: ", osInfo.Platform)
		return
	case *upgrade:
		cli.CheckAndUpgrade(NextVersion)
		return
	}

	if err := config.LoadEx(&conf, *confName); err != nil {
		log.Fatal("config.Load failed:", err)
	}

	if conf.TimeLayouts != nil {
		times.AddLayout(conf.TimeLayouts)
	}
	if conf.MaxProcs == 0 {
		conf.MaxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(conf.MaxProcs)
	log.SetOutputLevel(conf.DebugLevel)

	stopRotate := make(chan struct{}, 0)
	defer close(stopRotate)
	if conf.LogPath != "" {
		logdir, logpattern, err := utils.LogDirAndPattern(conf.LogPath)
		if err != nil {
			log.Fatal(err)
		}
		go loopRotateLogs(filepath.Join(logdir, logpattern), defaultRotateSize, 10*time.Second, stopRotate)
		conf.CleanSelfPattern = logpattern + "-*"
		conf.CleanSelfDir = logdir
	}

	log.Infof("Welcome to use Logkit, Version: %v \n\nConfig: %#v", NextVersion, conf)
	m, err := mgr.NewManager(conf.ManagerConfig)
	if err != nil {
		log.Fatalf("NewManager: %v", err)
	}
	m.Version = NextVersion

	paths := getValidPath(conf.ConfsPath)
	if len(paths) <= 0 {
		log.Warnf("Cannot read or create any ConfsPath %v", conf.ConfsPath)
	}
	if err = m.Watch(paths); err != nil {
		log.Fatalf("watch path error %v", err)
	}
	m.RestoreWebDir()

	stopClean := make(chan struct{}, 0)
	defer close(stopClean)
	if conf.CleanSelfLog {
		go loopCleanLogkitLog(conf.CleanSelfDir, conf.CleanSelfPattern, conf.CleanSelfLogCnt, 10*time.Minute, stopClean)
	}
	//if len(conf.BindHost) > 0 {
	//	m.BindHost = conf.BindIP+":"+conf.BindPort
	//}
	if len(conf.BindPort) == 0 {
		m.BindHost = conf.BindIP+":"+DEFAULT_PORT
	}else{
		m.BindHost = conf.BindIP+":"+conf.BindPort
	}

	e := echo.New()
	e.Static("/", conf.StaticRootPath)

	// start rest service
	rs := mgr.NewRestService(m, e)

	rs.PostParserCheck()

	if conf.ProfileHost != "" {
		log.Printf("profile_host was open at %v", conf.ProfileHost)
		go func() {
			log.Println(http.ListenAndServe(conf.ProfileHost, nil))
		}()
	}
	if err = rs.Register(); err != nil {
		log.Fatalf("register master error %v", err)
	}

	tenant := conf.Tenant
	bindIP := conf.BindIP
	bindPort := strings.Split(rs.Address(),":")[1]
	sendBlogic(tenant,bindIP,bindPort)


	utils.WaitForInterrupt(func() {
		rs.Stop()
		if conf.CleanSelfLog {
			stopClean <- struct{}{}
		}
		m.Stop()
	})
}
