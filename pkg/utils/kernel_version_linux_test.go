package utils

import (
	"fmt"
	"testing"
)

func TestGetKernelVersion(t *testing.T) {
	fmt.Println(GetKernelVersion())
}
