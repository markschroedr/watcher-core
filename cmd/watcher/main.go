package main

import (
	"github.com/markschroedr/watcher-core/internal/cli"
	"os"
)

func main() { os.Exit(cli.Main(os.Args[1:])) }
