package main

import (
	"flag"
	"fmt"

	"github.com/lth-go/wiser-go/utils"
)

var DEFAULT_II_BUFFER_UPDATE_THRESHOLD = 2048
var DefaultDatabaseFile = "test.db"

var wikipediaDumpFile = flag.String("x", "", "wiki dump file")
var query = flag.String("q", "", "query args")

func main() {
	//
	// 解析参数
	//

	// -x wiki dump file
	// -q query args
	// database file
	flag.Parse()

	databaseFile := flag.Arg(0)
	if databaseFile == "" {
		databaseFile = DefaultDatabaseFile
	}

	ok, err := utils.PathExists(databaseFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 在构建索引时，若指定的数据库已存在则报错
	if *wikipediaDumpFile != "" && ok {
		fmt.Printf("%s is already exists.\n", databaseFile)
		return
	}

	//
	// 初始化全局环境
	//
	env, err := NewEnv(databaseFile, DEFAULT_II_BUFFER_UPDATE_THRESHOLD)
	if err != nil {
		fmt.Printf("create env failed: %s\n", err)
		return
	}

	utils.PrintTimeDiff()

	// 加载Wikipedia的词条数据
	if *wikipediaDumpFile != "" {

		// TODO: 压缩方法

		err = env.LoadWikipediaDump(*wikipediaDumpFile)
		if err != nil {
			fmt.Printf("load wikipedia dump err: %s\n", err)
			return
		}

		err = env.AddDocument("", "")
		if err != nil {
			fmt.Printf("TODO: reflush failed: %s\n", err)
		}
	}

	// 进行检索
	// if *query != "" {

	//     // TODO: 加载压缩方法

	//     env.indexedCount = env.dbGetDocumentCount()
	//     env.Search(*query)
	// }

	// clean all
	utils.PrintTimeDiff()
	return
}
