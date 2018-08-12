package utils

import (
	"os"
	"time"
	"fmt"
)

var preTime *time.Time

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func PrintTimeDiff() {

	currentTime := time.Now()

	if preTime != nil {
		timeDiff := currentTime.UnixNano() - preTime.UnixNano()
		fmt.Printf("[time] %s (diff %d)\n", currentTime, timeDiff)
	} else {
		fmt.Printf("[time] %s\n", currentTime)
	}

	preTime = &currentTime
}
