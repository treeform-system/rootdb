# RootDB

![Logo](https://github.com/treeform-system/rootdb/blob/main/logo.png "RootDB logo")

## Description

RootDB is an embedded persistent relational database. Similar to sqlite, RootDB is meant to be small and easy to use since it conforms to the built-in database/sql interface. Uses SQL language to interact with database through the database/sql interface. Written in pure Golang and no external dependencies (only those used in testing are imported) its easy to integrate similar to how you would with any other databases (ie. Mysql, sqlite etc.)

## Installation

pacakge can be installed with go get (go 1.22 or higher recommended)
```
go get github.com/treeform-system/rootdb
```

## Features

Currently in development so only supports a subset of ANSI SQL, primarily the basic commands, no subquery currently supported
* Select
* Insert
* Create

Some features still in development:
- [ ] WAL manager
- [ ] Fault tolerance and durability
- [ ] B+Tree indexing
- [ ] ACID compliance
- [ ] Transaction Manager
- [ ] All Golang types

## Example Usage

```golang
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/treeform-system/rootdb"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	fmt.Printf("Drivers=%#v\n", sql.Drivers())

	db, err := sql.Open("rootdb", "example")
	check(err)
	defer db.Close()

	err = db.Ping()
	check(err)

	_, err = db.Query("CREATE TABLE MyTable10 (column1 int Primary Key, name char(10),column30 bool, column400 float, column5 int);")
	check(err)

	_, err = db.Query(`INSERT INTO "MyTable10" (column1,name,column30,column400,column5) VALUES
	 (1,'somecharss', true,1.23,2),
	 (2,'10letters', false,4.69,2),
	 (3,'Kevin', true,'.567',2),
	 (4,'tim', false,.678,2),
	 (5,'stringsss', true,5.36,3);`)
	check(err)	

	rows, err := db.Query("SELECT * FROM MyTable10;")
	check(err)
	fmt.Println("Select Query passed")

	for rows.Next() {
		var i int
		var n string
		var b bool
		var f float64
		err := rows.Scan(&i, &n, &b, &f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("(%d) (%s) (%t) (%f)\n", i, n, b, f)
	}

	rows, err = db.Query("SELECT * FROM MyTable10 WHERE column30 = true AND column1 > 1 AND column5 > 2;")
	check(err)
	for rows.Next() {
		var i int
		var n string
		var b bool
		var f float64
		var ii int
		err := rows.Scan(&i, &n, &b, &f, &ii)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("(%d) (%s) (%t) (%f) (%d)\n", i, n, b, f, ii)
	}
}
```

## License
[license](./LICENSE)