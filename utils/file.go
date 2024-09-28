package utils

import (
	"os"
)

func GetSubDirectories(dir string) ([]string, error) {
	var subDirectories []string

	// 读取目录内容
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// 过滤出子目录
	for _, file := range files {
		if file.IsDir() {
			subDirectories = append(subDirectories, file.Name())
		}
	}

	return subDirectories, nil
}

func FileIsExisted(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}
