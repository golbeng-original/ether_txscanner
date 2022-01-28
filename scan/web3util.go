package scan

import (
	"strings"
)

func IsRetryableWeb3Error(err error) bool {

	if err != nil && err.Error() == "no free connections available to host" {
		return true
	} else if err != nil && err.Error() == "dialing to the given TCP address timed out" {
		return true
	} else if err != nil && strings.Contains(err.Error(), "write: broken pipe") {
		return true
	} else if err != nil && strings.Contains(err.Error(), "connect: connection reset by peer") {
		return true
	} else if err != nil && strings.Contains(err.Error(), "response header before closing the connection") {
		return true
	} else if err != nil && strings.Contains(err.Error(), "socket: too many open files") {
		return true
	}

	//socket: too many open files

	return false
}
