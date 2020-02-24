package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func printPrompt() {
	fmt.Printf("db > ")
}

func readInput(r io.Reader) (string, error) {
	var val string
	count, err := fmt.Fscanln(r, &val)
	if count > 0 {
		// remove the last newline
		val = val[:len(val)-2]
	}
	return val, err
}

func main() {
	printPrompt()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {

		switch input := scanner.Text(); input {
		case ".exit":
			os.Exit(0)
		default:
			fmt.Printf("Unrecognized command '%s'.\n", input)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("error reading input: %v\n", err)
		os.Exit(1)
	}
}
