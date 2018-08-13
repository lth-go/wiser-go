package main

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"os"

	"github.com/lth-go/wiser-go/db"
	"github.com/lth-go/wiser-go/utils"
)

var NGram = 2

type Page struct {
	Title string `xml:"title"`
	Text  string `xml:"revision>text"`
}

// InvertedIndex 倒排文件
type InvertedIndex struct {
	PostingsMap map[int][]int

	// 该词元在所有文档中的出现次数之和
	PositionsCount int
}

type WiserEnv struct {
	DBPath string

	TokenLen int

	IIBuffer                map[int]InvertedIndex
	IIBufferUpdateThreshold int
	IndexedCount            int

	DB      *sql.DB
	DBStore *db.DBStore
}

func NewEnv(dbPath string, IIBufferUpdateThreshold int) (*WiserEnv, error) {
	var err error
	env := &WiserEnv{
		TokenLen:                NGram,
		IIBufferUpdateThreshold: IIBufferUpdateThreshold,
		IIBuffer:                map[int]InvertedIndex{},
		DBStore:                 &db.DBStore{},
	}

	err = env.DBStore.InitDatabase(dbPath)
	if err != nil {
		fmt.Printf("init database failed: %s\n", err)
	}

	return env, nil
}

//
// db
//

func (env *WiserEnv) DBGetDocumentID(title string) int {
	id := env.DBStore.GetDocumentId(title)
	return id
}
func (env *WiserEnv) DBGetDocumentTitle(id int) string {
	title := env.DBStore.GetDocumentTitle(id)
	return title
}
func (env *WiserEnv) DBAddDocument(title, body string) {
	id := env.DBGetDocumentID(title)

	if id != 0 {
		env.DBStore.UpdateDocument(id, body)

	} else {
		env.DBStore.InsertDocument(title, body)
	}
}

func (env *WiserEnv) DBGetTokenID(token string, insert int) (int, int) {
	if insert != 0 {
		env.DBStore.StoreToken(token, "")
	}

	id, count := env.DBStore.GetTokenId(token)
	return id, count
}
func (env *WiserEnv) DBGetToken(id int) string {
	token := env.DBStore.GetToken(id)
	return token
}
func (env *WiserEnv) DBGetPostings(id int) (int, string) {
	count, postings := env.DBStore.GetPostings(id)
	return count, postings
}
func (env *WiserEnv) DBUpdatePostings(id int, count int, postings string) {
	env.DBStore.UpdatePostings(id, count, postings)
}
func (env *WiserEnv) DBGetSettings(key string) string {
	value := env.DBStore.GetSettings(key)
	return value
}
func (env *WiserEnv) DBReplaceSettings(key, value string) {
	env.DBStore.ReplaceSettings(key, value)
}
func (env *WiserEnv) DBGetDocumentCount() int {
	count := env.DBStore.GetDocumentCount()
	return count
}

//
// 文档添加操作
//

// 将文档添加到数据库中，建立倒排索引
func (env *WiserEnv) AddDocument(title, body string) error {

	if title != "" && body != "" {
		// 添加到数据库中
		env.DBAddDocument(title, body)

		documentID := env.DBGetDocumentID(title)

		//  创建倒排列表
		err := env.TextToPostingsLists(documentID, body)
		if err != nil {
			return err
		}

		env.IndexedCount++
		fmt.Printf("count:%d title: %s\n", env.IndexedCount, title)
	}

	// 存储在缓冲区中的文档数量达到了指定的阈值时，更新存储器上的倒排索引
	if len(env.IIBuffer) > env.IIBufferUpdateThreshold || title == "" {
		utils.PrintTimeDiff()
		// TODO:
		err := env.UpdatePostingsAndFree()
		if err != nil {
			return err
		}
		utils.PrintTimeDiff()

	}
	return nil
}

func (env *WiserEnv) UpdatePostingsAndFree() error {
	for tokenID, II := range env.IIBuffer {
		oldPostings, err := env.FetchPostings(tokenID)
		if err != nil {
			return err
		}

		if oldPostings != nil {
			II.PostingsMap = utils.MergePositings(oldPostings, II.PostingsMap)
		}
		buf, err := utils.EncodePostings(II.PostingsMap)
		if err != nil {
			return err
		}
		env.DBUpdatePostings(tokenID, len(II.PostingsMap), buf)
	}

	env.IIBuffer = map[int]InvertedIndex{}
	fmt.Printf("index flushed\n")
	return nil
}

// FetchPostings 根据token id从数据库中获取位置信息
func (env *WiserEnv) FetchPostings(tokenID int) (map[int][]int, error) {
	_, buf := env.DBGetPostings(tokenID)

	if buf == "" {
		return nil, nil
	}

	postings, err := utils.DecodePostings(buf)
	if err != nil {
		return nil, err
	}

	return postings, nil
}

func (env *WiserEnv) TextToPostingsLists(id int, body string) error {
	// 分割N-gram词元
	runeBody := []rune(body)

	start := 0
	for {
		tokenLen, position := utils.NgramNext(runeBody, &start, env.TokenLen)

		if tokenLen == 0 {
			break
		}

		if tokenLen < env.TokenLen {
			continue
		}

		// 将词元添加到倒排列表中
		token := string(runeBody[position : position+env.TokenLen])

		err := env.TokenToPostingsList(id, token, start)
		if err != nil {
			return err
		}
	}
	return nil
}

func (env *WiserEnv) TokenToPostingsList(id int, token string, start int) error {
	tokenID, _ := env.DBGetTokenID(token, id)

	IIEntry, ok := env.IIBuffer[tokenID]
	if !ok {
		IIEntry = InvertedIndex{
			PostingsMap: map[int][]int{},
		}
		env.IIBuffer[tokenID] = IIEntry
	}
	_, ok = IIEntry.PostingsMap[id]
	if !ok {
		IIEntry.PostingsMap[id] = []int{}
	}

	IIEntry.PostingsMap[id] = append(IIEntry.PostingsMap[id], start)
	return nil
}

// LoadWikipediaDump 导入wikpedia数据
func (env *WiserEnv) LoadWikipediaDump(wikipediaDumpFile string) error {

	xmlFile, err := os.Open(wikipediaDumpFile)
	if err != nil {
		return err
	}
	defer xmlFile.Close()

	decoder := xml.NewDecoder(xmlFile)

	for {
		t, err := decoder.Token()

		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		if t == nil {
			break
		}

		switch se := t.(type) {
		case xml.StartElement:

			if se.Name.Local == "page" {
				var p Page
				decoder.DecodeElement(&p, &se)

				err = env.AddDocument(p.Title, p.Text)
				if err != nil {
					fmt.Printf("add document failed: %s\n", err)
					return err
				}
			}
		}
	}
	return nil
}
