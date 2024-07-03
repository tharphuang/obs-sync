package utils

import (
	"strconv"
	"strings"
	"syscall"
)

func GetKernelVersion() (major, minor int) {
	var uname syscall.Utsname
	if err := syscall.Uname(&uname); err == nil {
		buf := make([]byte, 0, 65) // Utsname.Release [65]int8
		for _, v := range uname.Release {
			if v == 0x00 {
				break
			}
			buf = append(buf, byte(v))
		}
		ps := strings.SplitN(string(buf), ".", 3)
		if len(ps) < 2 {
			return
		}
		if major, err = strconv.Atoi(ps[0]); err != nil {
			return
		}
		minor, _ = strconv.Atoi(ps[1])
	}
	return
}
