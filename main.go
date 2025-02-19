package main

import (
	"fmt"

	"dbfs/btree"
)

func main() {
	fmt.Println("Creating new tree...")
	btree.K()
	myTree:=btree.NewC()
	myTree.Add("0", "Aviral hereee")
	//fmt.Printf("Tree created: %+v\n", mytree)
}