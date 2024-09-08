//                                                                                                                         
// RRRRRRRRRRRRRRRRR                                              tttt               DDDDDDDDDDDDD      BBBBBBBBBBBBBBBBB   
// R::::::::::::::::R                                          ttt:::t               D::::::::::::DDD   B::::::::::::::::B  
// R::::::RRRRRR:::::R                                         t:::::t               D:::::::::::::::DD B::::::BBBBBB:::::B 
// RR:::::R     R:::::R                                        t:::::t               DDD:::::DDDDD:::::DBB:::::B     B:::::B
//   R::::R     R:::::R   ooooooooooo      ooooooooooo   ttttttt:::::ttttttt           D:::::D    D:::::D B::::B     B:::::B
//   R::::R     R:::::R oo:::::::::::oo  oo:::::::::::oo t:::::::::::::::::t           D:::::D     D:::::DB::::B     B:::::B
//   R::::RRRRRR:::::R o:::::::::::::::oo:::::::::::::::ot:::::::::::::::::t           D:::::D     D:::::DB::::BBBBBB:::::B 
//   R:::::::::::::RR  o:::::ooooo:::::oo:::::ooooo:::::otttttt:::::::tttttt           D:::::D     D:::::DB:::::::::::::BB  
//   R::::RRRRRR:::::R o::::o     o::::oo::::o     o::::o      t:::::t                 D:::::D     D:::::DB::::BBBBBB:::::B 
//   R::::R     R:::::Ro::::o     o::::oo::::o     o::::o      t:::::t                 D:::::D     D:::::DB::::B     B:::::B
//   R::::R     R:::::Ro::::o     o::::oo::::o     o::::o      t:::::t                 D:::::D     D:::::DB::::B     B:::::B
//   R::::R     R:::::Ro::::o     o::::oo::::o     o::::o      t:::::t    tttttt       D:::::D    D:::::D B::::B     B:::::B
// RR:::::R     R:::::Ro:::::ooooo:::::oo:::::ooooo:::::o      t::::::tttt:::::t     DDD:::::DDDDD:::::DBB:::::BBBBBB::::::B
// R::::::R     R:::::Ro:::::::::::::::oo:::::::::::::::o      tt::::::::::::::t     D:::::::::::::::DD B:::::::::::::::::B 
// R::::::R     R:::::R oo:::::::::::oo  oo:::::::::::oo         tt:::::::::::tt     D::::::::::::DDD   B::::::::::::::::B  
// RRRRRRRR     RRRRRRR   ooooooooooo      ooooooooooo             ttttttttttt       DDDDDDDDDDDDD      BBBBBBBBBBBBBBBBB   
//                                                                                                                          
                                                                                                              
package databasego

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sync"

	internal "github.com/treeform-system/rootdb/internal"
)

// Implements sql driver interface for opening database and returning connection to database
type Driver struct {
	bkd *internal.Backend

	mx *sync.Mutex
}

func init() {
	sql.Register("rootdb", &Driver{mx: new(sync.Mutex)})
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	fmt.Println("opening database")

	if d.bkd == nil {
		_, err := os.Stat(name)
		if os.IsNotExist(err) {
			err = os.Mkdir(name, 0755)
			if err != nil {
				return nil, err
			}

			d.bkd = internal.CreateNewDatabase(name)
		} else if err != nil {
			return nil, driver.ErrBadConn
		} else {
			tempdb, err := internal.OpenExistingDatabase(name)
			if err != nil {
				return nil, err
			}
			d.bkd = tempdb
		}

	}

	return &Conn{db: d.bkd}, nil
}

// Connection to the database
type Conn struct {
	db *internal.Backend
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Prepare not implemented")
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin not implemented")
}

// https://cs.opensource.google/go/go/+/refs/tags/go1.22.6:src/database/sql/sql.go;l=675
// seems to only call close once for final close
func (c *Conn) Close() error {
	c.db.Close()
	return nil
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		// TODO: support parameterization
		panic("Parameterization not supported")
	}

	ast, err := internal.Parse(query) //check if query for tablename is too long must be less than 16bits
	if err != nil {
		return nil, fmt.Errorf("error while parsing: %s", err)
	}

	// NOTE: ignorning all but the first statement
	stmt := ast.Type
	switch stmt {
	case internal.Create:
		err := c.db.CreateTable(ast)
		return nil, err
	case internal.Select:
		rows, err := c.db.Select(ast)
		if err != nil {
			return nil, err
		}
		return rows, nil
	case internal.Insert:
		err := c.db.Insert(ast)
		return nil, err
	default:
		return nil, errors.ErrUnsupported
	}
}
