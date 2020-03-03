package db

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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
	ExecuteFailedFile
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
	return fmt.Sprintf("(%d, %s, %s)", r.ID-1, r.Username[:userLen], r.Email[:emailLen])
}

func DeseralizeRow(source *[RowSize]byte) *Row {
	return (*Row)(unsafe.Pointer(source))
}

type Page [RowsPerPage][RowSize]byte
type Pager struct {
	backing *os.File
	Length  int64
	pages   [TableMaxPages]*Page
}

func (p *Pager) Get(pageNum int) (*Page, error) {
	var (
		pageByte [PageSize]byte
	)
	if pageNum > TableMaxPages {
		return nil, fmt.Errorf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TableMaxPages)
	}
	page := p.pages[pageNum]
	var numberOfPages = p.Length / PageSize
	if page != nil {
		return page, nil
	}

	// Cache miss, Allocate memory and load from file
	page = new(Page)

	// We might save a partial page at the end of the file
	if p.Length%PageSize != 0 {
		numberOfPages++
	}

	if int64(pageNum) < numberOfPages {
		// Need to load the page from the disk
		bytesRead, err := p.backing.ReadAt(pageByte[:], int64(pageNum*PageSize))
		if err != nil && err != io.EOF {
			return nil, err
		}
		// convert to a page
		for row := 0; row < int(RowsPerPage); row++ {
			rowOffset := row * int(RowSize)
			if rowOffset >= bytesRead {
				break
			}
			copy(page[row][:], pageByte[rowOffset:])
		}
	}

	p.pages[pageNum] = page
	return page, nil
}

func (p *Pager) Flush(pageNum int) error {
	var (
		pageByte [PageSize]byte
	)
	if pageNum > TableMaxPages {
		return fmt.Errorf("Tried to flush page number out of bounds. %d > %d\n", pageNum, TableMaxPages)
	}
	page := p.pages[pageNum]
	if page == nil {
		// nothing to do, page was never loaded from disk
		return nil
	}
	// flatten to bytes
	for row := 0; row < int(RowsPerPage); row++ {

		copy(pageByte[row*int(RowSize):], page[row][:])
	}
	//	p.backing.Seek(int64(pageNum)*PageSize, 0)
	_, err := p.backing.WriteAt(pageByte[:], int64(pageNum)*PageSize)
	if err != nil {
		return err
	}
	return nil

}
func (p *Pager) numberOfRowsOnDisk() int {
	var (
		pageByte [PageSize]byte
		rowByte  [RowSize]byte
	)
	if p.Length == 0 {
		return 0
	}
	var numberOfPages = (p.Length / PageSize)
	var lastPageOffset = (numberOfPages - 1) * PageSize
	p.backing.Seek(lastPageOffset, 0)
	bytesRead, err := p.backing.ReadAt(pageByte[:], lastPageOffset)
	if err != nil && err != io.EOF {
		panic(err)
	}
	numRows := 0

	if bytesRead == 0 {
		return int((numberOfPages-1)/int64(RowsPerPage)) + numRows
	}
	for i := 0; i < int(RowsPerPage); i++ {
		// check to see if the first byte is != 0
		start := i * int(RowSize)
		end := start + int(RowSize)
		copy(rowByte[:], pageByte[start:end])
		row := DeseralizeRow(&rowByte)
		// the first row with an id of zero we know the row of the
		// rows are not filled in
		if row.ID == 0 {
			break
		}
		numRows++
	}
	return int((numberOfPages-1)/int64(RowsPerPage)) + numRows

}

func (p *Pager) SyncToDisk() error {
	for i := range p.pages {
		if err := p.Flush(i); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) Close() error {
	if p == nil || p.backing == nil {
		return nil
	}
	// write out rows to disk
	if err := p.SyncToDisk(); err != nil {
		return err
	}

	err := p.backing.Close()
	p.backing = nil
	return err
}

func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0744)
	if err != nil {
		return nil, err
	}
	length, err := file.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	return &Pager{
		backing: file,
		Length:  length,
	}, nil
}

type Cursor struct {
	table      *Table
	rowNumber  uint32
	EndOfTable bool
}

func (cur *Cursor) Advance() {
	if cur == nil {
		return
	}
	cur.rowNumber++
	if cur.rowNumber >= cur.table.NumRows {
		cur.EndOfTable = true
	}
}

func (cur *Cursor) Value() (*[RowSize]byte, error) {
	if cur == nil {
		return nil, errors.New("cur is nil")
	}
	var (
		rowNum    = cur.rowNumber
		pageNum   = rowNum / RowsPerPage
		rowOffset = rowNum % RowsPerPage
	)
	page, err := cur.table.Pager.Get(int(pageNum))
	if err != nil {
		return nil, err
	}
	return &(page[rowOffset]), nil

}

type Table struct {
	NumRows uint32
	Pager   *Pager
}

func (tbl *Table) RowSlot(rowNum uint32) (*[RowSize]byte, error) {
	var (
		pageNum   = rowNum / RowsPerPage
		rowOffset = rowNum % RowsPerPage
	)
	page, err := tbl.Pager.Get(int(pageNum))
	if err != nil {
		return nil, err
	}
	return &(page[rowOffset]), nil
}

func (tbl *Table) insertRow(rowNum uint32, row *Row) error {
	pageNum := rowNum / RowsPerPage
	rowOffset := rowNum % RowsPerPage
	page, err := tbl.Pager.Get(int(pageNum))
	if err != nil {
		return err
	}
	page[rowOffset] = row.Seralize()
	return nil
}

func (tbl *Table) Close() (err error) {
	defer func() {
		if err != nil {
			log.Printf("got err: %v", err)
		}
	}()
	if tbl == nil {
		return nil
	}

	if err = tbl.Pager.Close(); err != nil {
		return err
	}
	return nil
}

func (tbl *Table) CursorAtStart() *Cursor {
	return &Cursor{
		table:      tbl,
		rowNumber:  0,
		EndOfTable: tbl.NumRows == 0,
	}
}

func (tbl *Table) CursorAtEnd() *Cursor {
	return &Cursor{
		table:      tbl,
		rowNumber:  tbl.NumRows,
		EndOfTable: true,
	}
}

func DBOpen(filename string) (*Table, error) {
	pager, err := NewPager(filename)
	if err != nil {
		return nil, err
	}
	numberOfRows := uint32(pager.numberOfRowsOnDisk())
	// numberOfRows may be too big, we need to see if
	// the last page only has a few rows.
	return &Table{
		NumRows: numberOfRows,
		Pager:   pager,
	}, nil
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

		r := Row{ID: uint32(id + 1)}
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
	cursor := tbl.CursorAtStart()
	for !cursor.EndOfTable {

		rowbyte, err := cursor.Value() //tbl.RowSlot(i)
		if err != nil {
			fmt.Fprintf(out, "failed to get row, %v", err)
			return ExecuteFailedFile
		}
		row := DeseralizeRow(rowbyte)
		fmt.Fprintln(out, row)

		cursor.Advance()
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

func Main(stdout, stderr io.Writer, stdin io.Reader, args []string) int {
	if len(args) != 2 {
		fmt.Fprintf(stderr, "Must supply a database filename.\n")
		return 2
	}

	table, err := DBOpen(args[1])
	if err != nil {
		fmt.Fprintf(stderr, "Failed to open database file(%v): %v", args[1], err)
		return 2
	}
	defer table.Close()

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
