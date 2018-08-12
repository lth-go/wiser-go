package main

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lth-go/wiser-go/utils"
)

var N_GRAM = 2

type Page struct {
	Title string `xml:"title"`
	Text  string `xml:"revision>text"`
}

// 倒排文件
type InvertedIndex struct {
	// TODO: remove
	TokenID     int
	PostingsMap map[int][]int

	// TODO: remove
	DocCount int
	// 该词元在所有文档中的出现次数之和
	PositionsCount int
}

type WiserEnv struct {
	DBPath string

	TokenLen int

	IIBuffer                map[int]InvertedIndex
	IIBufferUpdateThreshold int
	IndexedCount            int

	DB *sql.DB
	// DBStore
}

func NewEnv(dbPath string, IIBufferUpdateThreshold int) (*WiserEnv, error) {
	var err error
	env := &WiserEnv{
		TokenLen:                N_GRAM,
		IIBufferUpdateThreshold: IIBufferUpdateThreshold,
		IIBuffer: map[int]InvertedIndex{},
	}

	err = env.InitDatabase(dbPath)
	if err != nil {
		fmt.Printf("init database failed: %s\n", err)
	}

	return env, nil
}

func (env *WiserEnv) InitDatabase(dbPath string) error {
	var err error

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	env.DB = db

	env.DB.Exec("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);")
	env.DB.Exec("CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL);")
	env.DB.Exec("CREATE TABLE tokens (id INTEGER PRIMARY KEY, token TEXT NOT NULL, docs_count INT NOT NULL, postings BLOB NOT NULL);")
	env.DB.Exec("CREATE UNIQUE INDEX token_index ON tokens(token);")
	env.DB.Exec("CREATE UNIQUE INDEX title_index ON documents(title);")

	return nil
}

//
// 数据库操作
//

// document

func (env *WiserEnv) GetDocumentId(title string) int {
	stmt := "SELECT id FROM documents WHERE title = ?;"
	row := env.DB.QueryRow(stmt, title)

	var id int
	err := row.Scan(&id)

	if err == sql.ErrNoRows {
		return 0
	}

	if err != nil {
		panic(err)
	}

	return id
}

func (env *WiserEnv) GetDocumentTitle(id int) string {
	stmt := "SELECT title FROM documents WHERE id = ?;"
	row := env.DB.QueryRow(stmt, id)

	var title string
	err := row.Scan(&title)

	if err == sql.ErrNoRows {
		return ""
	}

	if err != nil {
		panic(err)
	}

	return title
}

func (env *WiserEnv) InsertDocument(title, body string) {
	stmt := "INSERT INTO documents (title, body) VALUES (?, ?);"

	res, err := env.DB.Exec(stmt, title, body)
	if err != nil {
		panic(err)
	}

	affect, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}

	if affect == 0 {
		panic(errors.New("insert document failed"))
	}
}
func (env *WiserEnv) UpdateDocument(id int, body string) {
	stmt := "UPDATE documents set body = ? WHERE id = ?;"

	_, err := env.DB.Exec(stmt, body, id)
	if err != nil {
		panic(err)
	}
}

func (env *WiserEnv) GetDocumentCount() int {
	stmt := "SELECT COUNT(*) FROM documents;"

	row := env.DB.QueryRow(stmt)

	var count int
	err := row.Scan(&count)

	if err == sql.ErrNoRows {
		return 0
	}

	if err != nil {
		panic(err)
	}

	return count
}

// token

func (env *WiserEnv) GetTokenId(token string) (int, int) {
	stmt := "SELECT id, docs_count FROM tokens WHERE token = ?;"

	row := env.DB.QueryRow(stmt, token)

	var id int
	var count int

	err := row.Scan(&id, &count)
	if err == sql.ErrNoRows {
		return 0, 0
	}
	if err != nil {
		panic(err)
	}

	return id, count

}

func (env *WiserEnv) GetToken(id int) string {
	stmt := "SELECT token FROM tokens WHERE id = ?;"

	row := env.DB.QueryRow(stmt, id)

	var token string

	err := row.Scan(&token)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		panic(err)
	}

	return token

}
func (env *WiserEnv) StoreToken(token, postings string) {
	stmt := "INSERT OR IGNORE INTO tokens (token, docs_count, postings) VALUES (?, 0, ?);"

	_, err := env.DB.Exec(stmt, token, postings)
	if err != nil {
		panic(err)
	}
}

// posting

func (env *WiserEnv) GetPostings(id int) (int, string) {
	stmt := "SELECT docs_count, postings FROM tokens WHERE id = ?;"

	row := env.DB.QueryRow(stmt, id)

	var count int
	var postings string

	err := row.Scan(&count, &postings)
	if err == sql.ErrNoRows {
		return 0, ""
	}
	if err != nil {
		panic(err)
	}

	return count, postings

}
func (env *WiserEnv) UpdatePostings(id int, count int, postings string) {
	stmt := "UPDATE tokens SET docs_count = ?, postings = ? WHERE id = ?;"

	_, err := env.DB.Exec(stmt, count, postings, id)
	if err != nil {
		panic(err)
	}
}
func (env *WiserEnv) GetSettings(key string) string {
	stmt := "SELECT value FROM settings WHERE key = ?;"

	row := env.DB.QueryRow(stmt, key)

	var value string

	err := row.Scan(&value)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		panic(err)
	}

	return value
}

// settings
func (env *WiserEnv) ReplaceSettings(key, value string) {
	stmt := "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?);"

	_, err := env.DB.Exec(stmt, key, value)
	if err != nil {
		panic(err)
	}
}

//
// db
//

func (env *WiserEnv) DBGetDocumentID(title string) int {
	id := env.GetDocumentId(title)
	return id
}
func (env *WiserEnv) DBGetDocumentTitle(id int) string {
	title := env.GetDocumentTitle(id)
	return title
}
func (env *WiserEnv) DBAddDocument(title, body string) {
	id := env.DBGetDocumentID(title)

	if id != 0 {
		env.UpdateDocument(id, body)

	} else {
		env.InsertDocument(title, body)
	}
}

func (env *WiserEnv) DBGetTokenID(token string, insert int) (int, int) {
	if insert != 0 {
		env.StoreToken(token, "")
	}

	id, count := env.GetTokenId(token)
	return id, count
}
func (env *WiserEnv) DBGetToken(id int) string {
	token := env.GetToken(id)
	return token
}
func (env *WiserEnv) DBGetPostings(id int) (int, string) {
	count, postings := env.GetPostings(id)
	return count, postings
}
func (env *WiserEnv) DBUpdatePostings(id int, count int, postings string) {
	env.UpdatePostings(id, count, postings)
}
func (env *WiserEnv) DBGetSettings(key string) string {
	value := env.GetSettings(key)
	return value
}
func (env *WiserEnv) DBReplaceSettings(key, value string) {
	env.ReplaceSettings(key, value)
}
func (env *WiserEnv) DBGetDocumentCount() int {
	count := env.GetDocumentCount()
	return count
}

//
//
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
	if env.IIBufferCount() > env.IIBufferUpdateThreshold || title == "" {
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
			II.PostingsMap = MergePositings(oldPostings, II.PostingsMap)
		}
		buf, err := env.EncodePostings(II.PostingsMap)
		if err != nil {
			return err
		}
		env.DBUpdatePostings(tokenID, II.DocCount, buf)
	}

	env.IIBuffer = map[int]InvertedIndex{}
	fmt.Printf("index flushed\n")
	return nil
}

func (env *WiserEnv) FetchPostings(tokenID int) (map[int][]int, error) {
	_, buf := env.DBGetPostings(tokenID)

	if buf == "" {
		return nil, nil
	}

	postings, err := env.DecodePostings(buf)
	if err != nil {
		return nil, err
	}

	return postings, nil
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

func (env *WiserEnv) TextToPostingsLists(id int, body string) error {
	// 分割N-gram词元
	runeBody := []rune(body)

	start := 0
	for {
		tokenLen, position := NgramNext(runeBody, &start, env.TokenLen)

		if tokenLen == 0 {
			break
		}

		if tokenLen < env.TokenLen {
			continue
		}

		// 将词元添加到倒排列表中
		token := string(runeBody[position: position+env.TokenLen])

		err := env.TokenToPostingsList(id, token, start)
		if err != nil {
			return err
		}
	}
	return nil
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

func (env *WiserEnv) TokenToPostingsList(id int, token string, start int) error {
	tokenID, _ := env.DBGetTokenID(token, id)

	IIEntry, ok := env.IIBuffer[tokenID]
	if !ok {
		IIEntry = InvertedIndex{
			TokenID:     tokenID,
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

func (env *WiserEnv) LoadWikipediaDump(wikipediaDumpFile string) error {

	xmlFile, err := os.Open(wikipediaDumpFile)
	if err != nil {
		return err
	}
	defer xmlFile.Close()

	decoder := xml.NewDecoder(xmlFile)

	for {
		t, err := decoder.Token()

		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
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

func (env *WiserEnv) EncodePostings(postingsMap map[int][]int) (string, error) {
	buf, err := json.Marshal(postingsMap)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (env *WiserEnv) DecodePostings(buf string) (map[int][]int, error) {
	postingsMap := map[int][]int{}

	err := json.Unmarshal([]byte(buf), postingsMap)
	if err != nil {
		return nil, err
	}

	return postingsMap, nil
}

func (env *WiserEnv) IIBufferCount() int {
	return len(env.IIBuffer)
}
