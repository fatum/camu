package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: camu <serve|test>\n")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		fmt.Println("camu serve: not yet implemented")
	case "test":
		fmt.Println("camu test: not yet implemented")
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
