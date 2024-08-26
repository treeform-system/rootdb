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

func printRows(rows *sql.Rows) {
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

func main() {
	fmt.Printf("Drivers=%#v\n", sql.Drivers())

	db, err := sql.Open("rootdb", "example")
	check(err)
	defer db.Close()

	fmt.Println("ping 1")
	err = db.Ping()
	check(err)

	_, err = db.Query("CREATE TABLE MyTable10 (column1 int Primary Key, name char(10),column30 bool, column400 float, column5 int);")
	check(err)
	fmt.Println("Create Query passed")

	_, err = db.Query(`INSERT INTO "MyTable10" (column1,name,column30,column400,column5) VALUES
	 (1,'somecharss', true,1.23,2),
	 (2,'10letters', false,4.69,2),
	 (3,'Kevin', true,'.567',2),
	 (4,'tim', false,.678,2),
	 (5,'stringsss', true,5.36,3);`)
	check(err)
	fmt.Println("Insert Queries passed")

	rows, err := db.Query("SELECT * FROM MyTable10;")
	check(err)
	fmt.Println("General Select Query passed")
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
	fmt.Println("select 0")

	rows, err = db.Query("SELECT * FROM MyTable10 WHERE column30 = true;")
	check(err)
	printRows(rows)
	fmt.Println("select 1")

	rows, err = db.Query("SELECT * FROM MyTable10 WHERE column1 >= 2;")
	check(err)
	printRows(rows)
	fmt.Println("non field comparison selects")

	rows, err = db.Query("SELECT * FROM MyTable10 WHERE column5 >= column1;")
	check(err)
	printRows(rows)
	fmt.Println("successfully compared fields")

	rows, err = db.Query("SELECT * FROM MyTable10 WHERE column30 = true AND column1 > 1 AND column5 > 2;")
	check(err)
	printRows(rows)

	rows, err = db.Query("SELECT * FROM MyTable10 WHERE column5 > 5;")
	check(err)
	printRows(rows)
	fmt.Println("Select Queries with where passed")

}
