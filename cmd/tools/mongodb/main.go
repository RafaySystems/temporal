package main

import (
	"os"

	"go.temporal.io/server/tools/mongodb"
)

func main() {
	if err := mongodb.RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}
