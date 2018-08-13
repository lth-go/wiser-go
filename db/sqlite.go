package db

import (
	"database/sql"
	"errors"

	_ "github.com/mattn/go-sqlite3"
)

type DBStore struct {
	db *sql.DB
}

func (dbStore *DBStore) InitDatabase(dbPath string) error {
	var err error

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	dbStore.db = db

	dbStore.db.Exec("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);")
	dbStore.db.Exec("CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL);")
	dbStore.db.Exec("CREATE TABLE tokens (id INTEGER PRIMARY KEY, token TEXT NOT NULL, docs_count INT NOT NULL, postings BLOB NOT NULL);")
	dbStore.db.Exec("CREATE UNIQUE INDEX token_index ON tokens(token);")
	dbStore.db.Exec("CREATE UNIQUE INDEX title_index ON documents(title);")

	return nil
}

//
// 数据库操作
//

// document

func (dbStore *DBStore) GetDocumentId(title string) int {
	stmt := "SELECT id FROM documents WHERE title = ?;"
	row := dbStore.db.QueryRow(stmt, title)

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

func (dbStore *DBStore) GetDocumentTitle(id int) string {
	stmt := "SELECT title FROM documents WHERE id = ?;"
	row := dbStore.db.QueryRow(stmt, id)

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

func (dbStore *DBStore) InsertDocument(title, body string) {
	stmt := "INSERT INTO documents (title, body) VALUES (?, ?);"

	res, err := dbStore.db.Exec(stmt, title, body)
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
func (dbStore *DBStore) UpdateDocument(id int, body string) {
	stmt := "UPDATE documents set body = ? WHERE id = ?;"

	_, err := dbStore.db.Exec(stmt, body, id)
	if err != nil {
		panic(err)
	}
}

func (dbStore *DBStore) GetDocumentCount() int {
	stmt := "SELECT COUNT(*) FROM documents;"

	row := dbStore.db.QueryRow(stmt)

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

func (dbStore *DBStore) GetTokenId(token string) (int, int) {
	stmt := "SELECT id, docs_count FROM tokens WHERE token = ?;"

	row := dbStore.db.QueryRow(stmt, token)

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

func (dbStore *DBStore) GetToken(id int) string {
	stmt := "SELECT token FROM tokens WHERE id = ?;"

	row := dbStore.db.QueryRow(stmt, id)

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
func (dbStore *DBStore) StoreToken(token, postings string) {
	stmt := "INSERT OR IGNORE INTO tokens (token, docs_count, postings) VALUES (?, 0, ?);"

	_, err := dbStore.db.Exec(stmt, token, postings)
	if err != nil {
		panic(err)
	}
}

// posting

func (dbStore *DBStore) GetPostings(id int) (int, string) {
	stmt := "SELECT docs_count, postings FROM tokens WHERE id = ?;"

	row := dbStore.db.QueryRow(stmt, id)

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
func (dbStore *DBStore) UpdatePostings(id int, count int, postings string) {
	stmt := "UPDATE tokens SET docs_count = ?, postings = ? WHERE id = ?;"

	_, err := dbStore.db.Exec(stmt, count, postings, id)
	if err != nil {
		panic(err)
	}
}
func (dbStore *DBStore) GetSettings(key string) string {
	stmt := "SELECT value FROM settings WHERE key = ?;"

	row := dbStore.db.QueryRow(stmt, key)

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
func (dbStore *DBStore) ReplaceSettings(key, value string) {
	stmt := "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?);"

	_, err := dbStore.db.Exec(stmt, key, value)
	if err != nil {
		panic(err)
	}
}
