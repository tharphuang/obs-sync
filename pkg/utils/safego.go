package utils

import (
	"fmt"
	"reflect"
	"runtime"
)

func SafeGo(fn func(), recoverHandler func(error)) {
	go func() {
		err := try2(fn, nil)
		if err != nil {
			recoverHandler(err)
		}
	}()
}

func try2(fn func(), cleaner func()) (ret error) {
	if cleaner != nil {
		defer cleaner()
	}
	defer func() {
		if err := recover(); err != nil {
			ret = fmt.Errorf("panic recover: func:%s err:%v", runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), err)
		}
	}()
	fn()
	return ret
}
