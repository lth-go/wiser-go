package utils

import (
	"fmt"
	"os"
	"time"

)

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

var preTime *time.Time

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

func IsIgnoredChar(c rune) bool {
	switch c {
	case ' ', '\f', '\n', '\r', '\t',
		'!', '"', '#', '$', '%', '&', '\'',
		'(', ')', '*', '+', ',', '-', '.',
		'/', ':', ';', '<', '=', '>', '?',
		'@', '[', '\\', ']', '^', '_', '`',
		'{', '|', '}', '~',
		'、', '。', '（', '）', '！', '，', '：', '；', '“', '”',
		'a', 'b', 'c', 'd', 'e', 'f', 'g',
		'h', 'i', 'j', 'k', 'l', 'm', 'n',
		'o', 'p', 'q', 'r', 's', 't',
		'u', 'v', 'w', 'x', 'y', 'z',
		'A', 'B', 'C', 'D', 'E', 'F', 'G',
		'H', 'I', 'J', 'K', 'L', 'M', 'N',
		'O', 'P', 'Q', 'R', 'S', 'T',
		'U', 'V', 'W', 'X', 'Y', 'Z',
		'1', '2', '3', '4', '5', '6', '7', '8', '9', '0':
		return true
	default:
		return false
	}
}

func NgramNext(runeBody []rune, start *int, n int) (int, int) {
	totalLen := len(runeBody)

	for {
		if *start >= totalLen {
			break
		}

		if !IsIgnoredChar(runeBody[*start]) {
			break
		}

		*start++
	}

	tokenLen := 0

	position := *start

	for {
		if *start >= totalLen {
			break
		}

		if tokenLen >= n {
			break
		}
		if IsIgnoredChar(runeBody[*start]) {
			break
		}

		*start++
		tokenLen++
	}

	// TODO:
	if tokenLen >= n {
		*start = position + 1
	}

	return tokenLen, position

}

func MergePositings(pa, pb map[int][]int) map[int][]int {
	mergeP := map[int][]int{}

	allKeysSet := NewSet()

	for key := range pa {
		allKeysSet.Add(key)
	}
	for key := range pb {
		allKeysSet.Add(key)
	}

	for _, key := range allKeysSet.List() {
		subSet := NewSet()

		al, ok := pa[key]
		if ok {
			subSet.Add(al...)
		}
		bl, ok := pb[key]
		if ok {
			subSet.Add(bl...)
		}

		mergeP[key] = subSet.SortList()
	}

	return mergeP
}
