package reader

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/qiniu/logkit/conf"
)

func NewFileAutoReader(conf conf.MapConf, meta *Meta, isFromWeb bool, bufSize int, whence string, logpath string, fr FileReader) (reader Reader, err error) {
	// for example: The path is "/usr/logkit/" or "F:\\user\\logkit\\" after==""
	// for example: The path is "/usr/logkit" or "F:\\user\\logkit"after==logkit
	_, after := filepath.Split(logpath)
	if after == "" {
		logpath = filepath.Dir(logpath)
	}
	//path with * matching tailx mode
	matchTailx := strings.Contains(logpath, "*")
	if matchTailx == true {
		meta.mode = ModeTailx
		expireDur, _ := conf.GetStringOr(KeyExpire, "24h")
		stateIntervalDur, _ := conf.GetStringOr(KeyStatInterval, "3m")
		maxOpenFiles, _ := conf.GetIntOr(KeyMaxOpenFiles, 256)
		reader, err = NewMultiReader(meta, logpath, whence, expireDur, stateIntervalDur, maxOpenFiles)
	} else {
		//for logpath this path to make judgments
		fileInfo, errStat := os.Stat(logpath)
		if errStat != nil {
			err = errStat
			return
		}
		if fileInfo.IsDir() == true {
			meta.mode = ModeDir
			ignoreHidden, _ := conf.GetBoolOr(KeyIgnoreHiddenFile, true)
			ignoreFileSuffix, _ := conf.GetStringListOr(KeyIgnoreFileSuffix, defaultIgnoreFileSuffix)
			validFilesRegex, _ := conf.GetStringOr(KeyValidFilePattern, "*")
			fr, err = NewSeqFile(meta, logpath, ignoreHidden, ignoreFileSuffix, validFilesRegex, whence)
			if err != nil {
				return
			}
			reader, err = NewReaderSize(fr, meta, bufSize)
		} else {
			meta.mode = ModeFile
			fr, err = NewSingleFile(meta, logpath, whence, isFromWeb)
			if err != nil {
				return
			}
			reader, err = NewReaderSize(fr, meta, bufSize)
		}
	}
	return
}
