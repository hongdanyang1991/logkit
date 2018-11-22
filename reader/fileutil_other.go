// +build !windows

package reader

import (
	"os"
	"reflect"
)

type StateOS struct {
	Inode uint64 `json:"inode,"`
	Device uint64 `json:"device,"`
}

type FileIdentity struct {
	Inode uint64 `json:"inode,"`
	Device uint64 `json:"device,"`
}

func GetOSState(info os.FileInfo) StateOS {
	stat := info.Sys().(*syscall.Stat_t)
	fileState := StateOS{
		Inode:  uint64(stat.Ino),
		Device: uint64(stat.Dev),
	}

	return fileState
}

func GetFileIdentity(info os.FileInfo) FileIdentity {
	state := GetOSState(info)
	identity := FileIdentity{
		Inode:  state.Inode,
		Device: state.Device,
	}
	return identity
}

func IsSameFile(file1 os.FileInfo, file2 os.FileInfo) bool {
	return GetFileIdentity(file1).Equals(GetFileIdentity(file2))
}

func (fid1 FileIdentity) Equals(fid2 FileIdentity) bool {
	return fid1 == fid2
}

func (fid FileIdentity) Stringify() string {
	return strconv.FormatUint(fid.Device, 36) + strconv.FormatUint(fid.Inode, 36)
}

func ReadOpen(path string) (f *os.File, err error) {
	return os.Open(path)
}
