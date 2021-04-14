package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	//"testing"
	"math/rand"
	//mbase "github.com/multiformats/go-multibase"
	"bytes"
	"encoding/binary"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"time"
)

var count int

func init() {
	os.Remove("/data/go_sqlite_monotonic_crud.db")

	db, err := sql.Open("sqlite3", "/data/go_sqlite_monotonic_crud.db")
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

	for i := 0; i < 100; i++ {
		randValue := make([]byte, 1024*100)
		rand.Read(randValue)
		data := IntToByte(i)
		hash, _ := mh.Sum(data, mh.SHA2_256, -1)

		cid := cid.NewCidV1(cid.Raw, hash)

		_, err = stmt.Exec(i, cid.Bytes(), randValue)
		if err != nil {
			log.Fatal(err)
		}
		count = count + 1
	}
	tx.Commit()
}

func IntToByte(num int) []byte {
	var buffer bytes.Buffer
	_ = binary.Write(&buffer, binary.BigEndian, num)
	return buffer.Bytes()
}

func main() {
	{
		t := time.Now()
		if err := testInsert(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("insert:", time.Since(t)/5000)
	}
	{
		t := time.Now()
		if err := testQuery(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("query:", time.Since(t)/5000)
	}
}

func testInsert() error {
	db, err := sql.Open("sqlite3", "/data/go_sqlite_monotonic_crud.db")
	if err != nil {
		return err
	}
	defer db.Close()

	for i := 0; i < 5000; i++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare("INSERT INTO record (id,key,value) VALUES (?,?,?)")
		if err != nil {
			return err
		}

		randValue := make([]byte, 1024*100)

		rand.Read(randValue)

		data := IntToByte(count)

		hash, _ := mh.Sum(data, mh.SHA2_256, -1)

		cid := cid.NewCidV1(cid.Raw, hash)

		_, err = stmt.Exec(count, cid.Bytes(), randValue)
		if err != nil {
			return err
		}
		tx.Commit()
		stmt.Close()
		count = count + 1
	}
	return nil
}

func testQuery() error {
	db, err := sql.Open("sqlite3", "/data/go_sqlite_monotonic_crud.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 5000; i++ {
		num := rand.Intn(count)
		data := IntToByte(num)
		hash, _ := mh.Sum(data, mh.SHA2_256, -1)

		cid := cid.NewCidV1(cid.Raw, hash)

		row := db.QueryRow(`SELECT id,key,value FROM record where key=$1`, cid.Bytes())
		if row == nil {
			fmt.Println("Failed to call db.QueryRow")
		}
	}
	return nil
}
