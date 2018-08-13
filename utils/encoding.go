package utils

import "encoding/json"

func EncodePostings(postingsMap map[int][]int) (string, error) {
	buf, err := json.Marshal(postingsMap)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func DecodePostings(buf string) (map[int][]int, error) {
	postingsMap := map[int][]int{}

	err := json.Unmarshal([]byte(buf), postingsMap)
	if err != nil {
		return nil, err
	}

	return postingsMap, nil
}
