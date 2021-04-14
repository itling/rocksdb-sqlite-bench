package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	cid "github.com/ipfs/go-cid"
	_ "github.com/mattn/go-sqlite3"
	mh "github.com/multiformats/go-multihash"
	"log"
	"math/rand"
	"os"
	"testing"
)

var count int

// 数据路径
var dataPath string = "/data/go_sqlite_monotonic_crud.db"

// 单位byte
var dataItemSize int = 1024 * 100

// 总大小=dataItemSize*totalLoopCount
var initLoopCount int = 100

func IntToByte(num int) []byte {
	var buffer bytes.Buffer
	_ = binary.Write(&buffer, binary.BigEndian, num)
	return buffer.Bytes()
}

func BenchmarkCid(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := IntToByte(i)
		hash, _ := mh.Sum(data, mh.SHA2_256, -1)
		cid.NewCidV1(cid.Raw, hash)
	}
}

func BenchmarkGenRandom(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randValue := make([]byte, dataItemSize)
		rand.Read(randValue)
	}
}

func init() {

	os.Remove(dataPath)

	db, err := sql.Open("sqlite3", dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sqlTableCreate := `
	CREATE TABLE IF NOT EXISTS  record (
		id              INTEGER primary key,
		key             BLOB NOT NULL,
		value           BLOB NOT NULL
		)
	`
	_, err = db.Exec(sqlTableCreate)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlTableCreate)
		return
	}

	sqlIndex := `
	CREATE index  IF NOT EXISTS index_record_id on record(id);
	CREATE index  IF NOT EXISTS index_record_key on record(key)
	`
	_, err = db.Exec(sqlIndex)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlIndex)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO record (id,key, value) VALUES (?, ?,?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < initLoopCount; i++ {
		randValue := make([]byte, dataItemSize)
		rand.Read(randValue)
		data := IntToByte(count)
		hash, _ := mh.Sum(data, mh.SHA2_256, -1)

		cid := cid.NewCidV1(cid.Raw, hash)

		_, err = stmt.Exec(count, cid.Bytes(), randValue)
		if err != nil {
			log.Fatal(err)
		}
		count = count + 1
	}
	tx.Commit()
}

func BenchmarkInsert(b *testing.B) {
	db, err := sql.Open("sqlite3", dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		stmt, err := tx.Prepare("INSERT INTO record (id,key,value) VALUES (?,?,?)")
		if err != nil {
			log.Fatal(err)
		}
		defer stmt.Close()

		randValue := make([]byte, dataItemSize)

		rand.Read(randValue)

		data := IntToByte(count)

		hash, _ := mh.Sum(data, mh.SHA2_256, -1)

		cid := cid.NewCidV1(cid.Raw, hash)

		_, err = stmt.Exec(count, cid.Bytes(), randValue)
		if err != nil {
			log.Fatal(err)
		}
		tx.Commit()
		count = count + 1
	}
}

func BenchmarkQuery(b *testing.B) {

	db, err := sql.Open("sqlite3", dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		num := rand.Intn(count)

		hash, _ := mh.Sum(IntToByte(num), mh.SHA2_256, -1)

		cid := cid.NewCidV1(cid.Raw, hash)

		row := db.QueryRow(`SELECT id,key,value FROM record where key=$1`, cid.Bytes())
		if row == nil {
			fmt.Println("Failed to call db.QueryRow")
		}
	}
}
