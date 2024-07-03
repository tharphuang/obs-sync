//go:build !windows
// +build !windows

package utils

import (
	"os"
	"syscall"
)

func GetDev(fpath string) int { // ID of device containing file
	fi, err := os.Stat(fpath)
	if err != nil {
		return -1
	}
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int(sst.Dev)
	}
	return -1
}
