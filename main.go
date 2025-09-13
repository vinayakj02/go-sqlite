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
)

type Row struct {
	id       uint32
	username [USERNAME_SIZE]byte
	email    [EMAIL_SIZE]byte
}

var table []Row

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
		fmt.Println("Syntax error. Usage: insert <id> <username> <email>")
		return
	}

	id, err := strconv.Atoi(query[1])
	if err != nil {
		fmt.Println("Invalid ID:", query[1])
		return
	}
	var row Row
	row.id = uint32(id)
	copy(row.username[:], query[2])
	copy(row.email[:], query[3])

	file, err := os.OpenFile("data.db", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	serialized_row := serializeRow(row)
	fmt.Println("Serialized Row:", serialized_row)

	_, err = file.Write(serialized_row)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

func selectRows() {
	file, err := os.Open("data.db")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	buffer := make([]byte, ROW_SIZE)
	for { // loops until buffer not multiple of ROW_SIZE
		n, err := file.Read(buffer)
		if err != nil {
			break
		}
		if n < ROW_SIZE { // End of file or incomplete row
			break
		}
		row := deserializeRow(buffer)
		fmt.Print(row)
		fmt.Printf("(%d, %s, %s)\n", row.id, strings.TrimRight(string(row.username[:]), "\x00"), strings.TrimRight(string(row.email[:]), "\x00"))
	}
}

func handle_META_COMMAND(query []string) {
	if query[0] == ".exit" {
		fmt.Println("Byeee...")
		syscall.Exit(0)
	} else if query[0] == ".help" {
		fmt.Println("This is a simple SQLite db written in go.")
	} else {
		fmt.Println("Unrecognized command ", query[0])
	}

}

func handle_SQL_COMMAND(query []string) {
	if strings.EqualFold(query[0], "select") {
		selectRows()
	} else if strings.EqualFold(query[0], "insert") {
		insertRow(query)
	} else {
		fmt.Println("Unrecognized command")
	}
}

func main() {
	fmt.Println("Welcome to goSQLite")
	scanner := bufio.NewScanner(os.Stdin)
	for true {
		fmt.Print("sqlite > ")
		if !scanner.Scan() {
			break
		}
		query := strings.Fields(strings.TrimSpace(scanner.Text()))

		if query[0] == "" || len(query) == 0 {
			continue
		} else if query[0][0] == '.' {
			handle_META_COMMAND(query)
		} else {
			handle_SQL_COMMAND(query)
		}
	}
}
