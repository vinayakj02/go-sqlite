package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const (
	USERNAME_SIZE = 32
	EMAIL_SIZE    = 255
	ROW_SIZE      = 4 + USERNAME_SIZE + EMAIL_SIZE
	DB_NAME       = "data.db"

	PAGE_SIZE     = 4096
	MAX_PAGES     = 100
	ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
)

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	pages          [MAX_PAGES]*Page
}

type Page struct {
	data [PAGE_SIZE]byte
}

type Row struct {
	id       uint32
	username [USERNAME_SIZE]byte
	email    [EMAIL_SIZE]byte
}

type Table struct {
	pager   *Pager
	numRows uint32
}

type Cursor struct {
	endOfTable bool
	table      *Table
	rowNum     uint32
}

func tableStart(table *Table) *Cursor {
	cursor := Cursor{
		endOfTable: table.numRows == 0,
		table:      table,
		rowNum:     0,
	}
	return &cursor
}

func tableEnd(table *Table) *Cursor {
	cursor := Cursor{
		endOfTable: true,
		table:      table,
		rowNum:     table.numRows,
	}
	return &cursor
}

func (c *Cursor) getCursorValue() []byte {
	return getRowSlot(c.table, c.rowNum)
}

func (c *Cursor) cursorAdvance() {
	c.rowNum++
	c.endOfTable = (c.rowNum >= c.table.numRows)
}

var globalTable *Table

func (p *Pager) flush(pageNum uint32) {
	if pageNum > MAX_PAGES {
		fmt.Println("page number out of bounds : ", pageNum)
		os.Exit(1)
	}

	if p.pages[pageNum] == nil {
		fmt.Println("page not found")
		return
	}

	page_offset := int64(pageNum * PAGE_SIZE)
	_, err := p.fileDescriptor.Seek(page_offset, 0)
	if err != nil {
		fmt.Println("error !! : ", err)
		return
	}

	_, err = p.fileDescriptor.Write(p.pages[pageNum].data[:])
	if err != nil {
		fmt.Println("error in writing to file :( : ", err)
	}
}
func newPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	fileLength := uint32(fileInfo.Size())

	pager := Pager{
		fileDescriptor: file,
		fileLength:     fileLength,
		pages:          [MAX_PAGES]*Page{},
	}

	return &pager, nil
}

func newTable(filename string) (*Table, error) {
	pager, err := newPager(filename)
	if err != nil {
		return nil, err
	}
	table := Table{
		pager:   pager,
		numRows: pager.fileLength / ROW_SIZE,
	}
	return &table, nil
}

func (p *Pager) getPage(pageNum uint32) *Page { // this is a method of the struct ?
	if pageNum >= MAX_PAGES {
		fmt.Println("page number out of bounds : ", pageNum)
		os.Exit(1)
	}

	if p.pages[pageNum] == nil {
		p.pages[pageNum] = &Page{}

		if pageNum < p.fileLength/PAGE_SIZE {
			offset := int64(pageNum * PAGE_SIZE)
			p.fileDescriptor.Seek(offset, 0)
			p.fileDescriptor.Read(p.pages[pageNum].data[:])
		}
	}
	return p.pages[pageNum]
}

func getRowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE
	page := table.pager.getPage(pageNum)
	rowOffset := (rowNum % ROWS_PER_PAGE) * ROW_SIZE
	return page.data[rowOffset : rowOffset+ROW_SIZE]
}

func serializeRow(row Row) []byte {
	buffer := make([]byte, 4+USERNAME_SIZE+EMAIL_SIZE)

	binary.LittleEndian.PutUint32(buffer[0:4], row.id)
	copy(buffer[4:4+USERNAME_SIZE], row.username[:])
	copy(buffer[4+USERNAME_SIZE:], row.email[:])
	return buffer
}

func deserializeRow(buffer []byte) Row {
	var row Row
	row.id = binary.LittleEndian.Uint32(buffer[0:4])
	copy(row.username[:], buffer[4:4+USERNAME_SIZE])
	copy(row.email[:], buffer[4+USERNAME_SIZE:])
	return row
}

func insertRow(query []string) {
	if len(query) != 4 {
		fmt.Println("syntax error -> usage: insert <id> <username> <email>")
		return
	}
	id, err := strconv.Atoi(query[1])
	if err != nil {
		fmt.Println("invalid id:", query[1])
		return
	}
	var row Row
	row.id = uint32(id)
	copy(row.username[:], query[2])
	copy(row.email[:], query[3])

	// destination := getRowSlot(globalTable, globalTable.numRows)
	destination := tableEnd(globalTable).getCursorValue()

	serialized_row := serializeRow(row)

	copy(destination, serialized_row)
	globalTable.numRows++
}

func selectRows() {
	// for i := range uint32(globalTable.numRows) {
	// 	source := getRowSlot(globalTable, uint32(i))
	// 	row := deserializeRow(source)
	// 	fmt.Printf("(%d, %s, %s)\n", row.id, strings.TrimRight(string(row.username[:]), "\x00"), strings.TrimRight(string(row.email[:]), "\x00"))
	// }
	cursor := tableStart(globalTable)
	fmt.Println(cursor.rowNum, cursor.endOfTable)
	for !cursor.endOfTable {
		row := deserializeRow(cursor.getCursorValue())
		fmt.Printf("(%d, %s, %s)\n", row.id, strings.TrimRight(string(row.username[:]), "\x00"), strings.TrimRight(string(row.email[:]), "\x00"))
		cursor.cursorAdvance()
	}
}

func dump_db() {
	err := os.Remove(DB_NAME)
	if err != nil {
		fmt.Println("error in dumping db :(")
		return
	}
}

func handle_META_COMMAND(query []string) {
	if query[0] == ".exit" {
		globalTable.close()
		fmt.Println("byeee...")
		syscall.Exit(0)
	} else if query[0] == ".help" {
		fmt.Println("this is a simple SQLite db written in go.")
	} else if query[0] == ".dump" {
		fmt.Println("dumping db...")
		dump_db()
	} else {
		fmt.Println("unrecognized command ", query[0])
	}

}

func handle_SQL_COMMAND(query []string) {
	if strings.EqualFold(query[0], "select") {
		selectRows()
	} else if strings.EqualFold(query[0], "insert") {
		insertRow(query)
	} else {
		fmt.Println("unrecognized command")
	}
}
func (t *Table) close() {
	numPages := ((t.numRows + ROWS_PER_PAGE - 1) / ROWS_PER_PAGE)
	for page_num := range uint32(numPages) {
		if t.pager.pages[page_num] != nil {
			t.pager.flush(page_num)
		}
	}
	t.pager.fileDescriptor.Close()
}

func main() {
	fmt.Println("welcome to goSQLite")
	var err error
	globalTable, err = newTable(DB_NAME)

	if err != nil {
		fmt.Println("error opening db : ", err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for true {
		fmt.Print("sqlite > ")
		if !scanner.Scan() {
			break
		}
		query := strings.Fields(strings.TrimSpace(scanner.Text()))

		if len(query) == 0 || query[0] == "" {
			continue
		} else if query[0][0] == '.' {
			handle_META_COMMAND(query)
		} else {
			handle_SQL_COMMAND(query)
		}
	}
}
