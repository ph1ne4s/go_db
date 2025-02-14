package main

import (
	"fmt"

	"github.com/ph1ne4s/go_db/btree"
)

func main() {
	fmt.Println("Creating new tree...")
	mytree := btree.NewC()
	fmt.Printf("Tree created: %+v\n", mytree)
}