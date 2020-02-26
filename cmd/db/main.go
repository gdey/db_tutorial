package main

import (
	"os"

	"github.com/gdey/db_tutorial/db"
)

func main() {
	os.Exit(db.Main(os.Stdout, os.Stderr, os.Stdin))
}
