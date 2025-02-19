package main

import (
	"fmt"

	"dbfs/btree"
)

func main() {
	fmt.Println("Creating new tree...")
	myTree:=btree.NewC()
	myTree.Add("0", "Aviral hereee")
	myTree.Add("1", "whyyy")
	fmt.Printf("Tree created: %+v\n", myTree)
}