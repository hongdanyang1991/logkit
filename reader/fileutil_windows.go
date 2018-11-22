package reader

import (
	"os"
	"reflect"
	"syscall"
	"strconv"
)

type StateOS struct {
	IdxHi uint64 `json:"idxhi,"`
	IdxLo uint64 `json:"idxlo,"`
	Vol   uint64 `json:"vol,"`
}

type FileIdentity struct {
	Inode uint64 `json:"inode,"`
	Device uint64 `json:"device,"`
}

func GetOSState(info os.FileInfo) StateOS {
	os.SameFile(info, info)
	fileStat := reflect.ValueOf(info).Elem()
	fileState := StateOS{
		IdxHi: uint64(fileStat.FieldByName("idxhi").Uint()),
		IdxLo: uint64(fileStat.FieldByName("idxlo").Uint()),
		Vol: uint64(fileStat.FieldByName("vol").Uint()),
	}

	return fileState
}

func GetFileIdentity(info os.FileInfo) FileIdentity {
	state := GetOSState(info)
	identity := FileIdentity{
		Inode:  GetInode(state),
		Device: state.Vol,
	}
	return identity
}

func GetInode(state StateOS) uint64 {
	return uint64(state.IdxHi<<32 + state.IdxLo)
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
	var pathp *uint16
	pathp, err = syscall.UTF16PtrFromString(path)
	if err != nil {
		return
	}

	var access uint32
	access = syscall.GENERIC_READ

	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)

	var sa *syscall.SecurityAttributes

	var createmode uint32

	createmode = syscall.OPEN_EXISTING

	var handle syscall.Handle
	handle,err = syscall.CreateFile(pathp, access, sharemode, sa, createmode, syscall.FILE_ATTRIBUTE_NORMAL, 0)

	if err != nil {
		return
	}

	f = os.NewFile(uintptr(handle), path)

	return
}
