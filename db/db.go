package db

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"unsafe"
)

type MetaCommand uint

const (
	MetaCommandSuccess MetaCommand = iota
	MetaCommandExit
	MetaCommandUnrecognizedCommand
)

type PrepareResult uint

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
	PrepareSyntaxError
	PrepareStringTooLong
	PrepareNegativeID
)

type ExecuteResult uint

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
)

type StatementType uint

const (
	StatementInsert StatementType = iota
	StatementSelect
)

const (
	ColumnUsernameSize = 32
	ColumnEmailSize    = 255
)

const (
	RowSize       = uint32(unsafe.Sizeof(Row{}))
	PageSize      = 4096
	TableMaxPages = 100
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages
)

type Row struct {
	ID       uint32
	Username [ColumnUsernameSize]byte
	Email    [ColumnEmailSize]byte
}

func (r Row) Seralize() [RowSize]byte {
	return (*(*[RowSize]byte)(unsafe.Pointer(&r)))
}

func (r Row) String() string {
	userLen := bytes.IndexByte(r.Username[:], 0)
	if userLen == -1 {
		userLen = ColumnUsernameSize
	}
	emailLen := bytes.IndexByte(r.Email[:], 0)
	if emailLen == -1 {
		emailLen = ColumnEmailSize
	}
	return fmt.Sprintf("(%d, %s, %s)", r.ID, r.Username[:userLen], r.Email[:emailLen])
}

func DeseralizeRow(source *[RowSize]byte) *Row {
	return (*Row)(unsafe.Pointer(source))
}

type Table struct {
	NumRows uint32
	Pages   [TableMaxPages][RowsPerPage][RowSize]byte
}

func (tbl *Table) RowSlot(rowNum uint32) *[RowSize]byte {
	pageNum := rowNum / RowsPerPage
	rowOffset := rowNum % RowsPerPage
	return &(tbl.Pages[pageNum][rowOffset])
}

func (tbl *Table) insertRow(rowNum uint32, row *Row) {
	pageNum := rowNum / RowsPerPage
	rowOffset := rowNum % RowsPerPage
	tbl.Pages[pageNum][rowOffset] = row.Seralize()
}

type Statement struct {
	Type StatementType
	// InsertRow is only used by insert statement
	InsertRow *Row
}

func printPrompt(out io.Writer) {
	fmt.Fprintf(out, "db > ")
}

func doMetaCommand(input string) MetaCommand {
	switch input {
	case ".exit":
		return MetaCommandExit
	default:
		return MetaCommandUnrecognizedCommand
	}
}

func prepareStatement(input string) (*Statement, PrepareResult) {
	switch {
	case strings.HasPrefix(input, "insert"):
		var (
			id       int
			username string
			email    string
		)
		_, err := fmt.Sscanf(input, "insert %d %s %s", &id, &username, &email)
		if err != nil {
			log.Printf("error: %v", err)
			return nil, PrepareSyntaxError
		}
		if len(username) > ColumnUsernameSize {
			return nil, PrepareStringTooLong
		}
		if len(email) > ColumnEmailSize {
			return nil, PrepareStringTooLong
		}

		if id < 0 {
			return nil, PrepareNegativeID
		}

		r := Row{ID: uint32(id)}
		copy(r.Username[:], []byte(username))
		copy(r.Email[:], []byte(email))

		return &Statement{
			Type:      StatementInsert,
			InsertRow: &r,
		}, PrepareSuccess
	case strings.HasPrefix(input, "select"):
		return &Statement{Type: StatementSelect}, PrepareSuccess
	default:
		return nil, PrepareUnrecognizedStatement
	}
}

func (tbl *Table) executeInsert(out io.Writer, statement *Statement) ExecuteResult {
	if tbl.NumRows >= TableMaxRows {
		return ExecuteTableFull
	}
	tbl.insertRow(tbl.NumRows, statement.InsertRow)
	tbl.NumRows += 1
	return ExecuteSuccess
}

func (tbl *Table) executeSelect(out io.Writer, statement *Statement) ExecuteResult {
	for i := uint32(0); i < tbl.NumRows; i++ {
		row := DeseralizeRow(tbl.RowSlot(i))
		fmt.Fprintln(out, row)
	}
	return ExecuteSuccess
}

func executeStatement(out io.Writer, statement *Statement, table *Table) ExecuteResult {
	if statement == nil || table == nil {
		return ExecuteSuccess
	}
	switch statement.Type {
	case StatementInsert:
		return table.executeInsert(out, statement)
	case StatementSelect:
		return table.executeSelect(out, statement)
	default:
		return ExecuteSuccess
	}
}

func Main(stdout, stderr io.Writer, stdin io.Reader) int {
	table := new(Table)
	scanner := bufio.NewScanner(stdin)
	for {
		printPrompt(stdout)
		scanner.Scan()

		input := scanner.Text()
		if input == "" {
			continue
		}

		if input[0] == '.' {
			switch doMetaCommand(input) {
			case MetaCommandExit:
				return 0
			case MetaCommandUnrecognizedCommand:
				fmt.Fprintf(stderr, "Unrecognized command '%s'.\n", input)
			case MetaCommandSuccess:
			}
			continue
		}

		statement, result := prepareStatement(input)
		switch result {
		case PrepareSuccess:
		// noop
		case PrepareSyntaxError:
			fmt.Fprintln(stderr, "Syntax error. Could not parse statement.")
			continue
		case PrepareStringTooLong:
			fmt.Fprintln(stderr, "String is too long.")
			continue
		case PrepareNegativeID:
			fmt.Fprintln(stderr, "ID must be positive.")
			continue
		case PrepareUnrecognizedStatement:
			fmt.Fprintf(stderr, "Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		switch executeStatement(stdout, statement, table) {
		case ExecuteSuccess:
			fmt.Fprintln(stdout, "Executed.")
		case ExecuteTableFull:
			fmt.Fprintln(stderr, "Error: Table full.")
		}

	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(stderr, "error reading input: %v\n", err)
		return 1
	}
	return 0
}
