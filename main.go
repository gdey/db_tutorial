package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
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
)

type StatementType uint

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type StatementType
}

func printPrompt() {
	fmt.Printf("db > ")
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
		return &Statement{Type: StatementInsert}, PrepareSuccess
	case strings.HasPrefix(input, "select"):
		return &Statement{Type: StatementSelect}, PrepareSuccess
	default:
		return nil, PrepareUnrecognizedStatement
	}
}

func executeStatement(statement *Statement) {
	if statement == nil {
		return
	}
	switch statement.Type {
	case StatementInsert:
		fmt.Printf("This is where we would do an insert.\n")
	case StatementSelect:
		fmt.Printf("This is where we would do an select.\n")
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		printPrompt()
		scanner.Scan()

		input := scanner.Text()
		if input == "" {
			continue
		}

		if input[0] == '.' {
			switch doMetaCommand(input) {
			case MetaCommandExit:
				os.Exit(0)
			case MetaCommandUnrecognizedCommand:
				fmt.Printf("Unrecognized command '%s'.\n", input)
			case MetaCommandSuccess:
			}
			continue
		}

		statement, result := prepareStatement(input)
		if result == PrepareUnrecognizedStatement {
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		executeStatement(statement)
		fmt.Printf("Executed.\n")

	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("error reading input: %v\n", err)
		os.Exit(1)
	}
}
