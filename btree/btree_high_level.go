package btree

func (tree *BTree) Insert(key []byte, val []byte) error {
	assert(len(key) == 0, "empty key") //check lengths by node format
	assert(len(key) > BTREE_MAX_KEY_SIZE, "key too big")
	assert(len(val) > BTREE_MAX_VAL_SIZE, "val too big")

	if tree.root == 0 { // create first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// dummy key(smallest key) for lookupLE func to find and take key space
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, val) //insert key

	nsplit, split := nodeSplit3(node) //grow tree if root split
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), uint64(ptr), key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
	return nil
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) { //shoulf updated child be merged with sibling?
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := BNode(tree.get(uint16(node.getPtr(idx - 1))))
		merged := sibling.nbytes() + updated.nbytes() - 4
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling //left
		}
	}
	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(uint16(node.getPtr(idx + 1))))
		merged := sibling.nbytes() + updated.nbytes() - 4
		if merged <= BTREE_PAGE_SIZE {
			return 1, sibling //right
		}
	}
	return 0, BNode{}
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode

// delete a key from an internal node; part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(uint16(kptr)), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(uint16(kptr))
	// check for merging
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(uint16(node.getPtr(idx - 1)))
		nodeReplace2Kid(new, node, idx-1, uint64(tree.new(merged)), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(uint16(node.getPtr(idx + 1)))
		nodeReplace2Kid(new, node, idx, uint64(tree.new(merged)), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0, "1 empty child but no sibling") // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)                                          // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// helper to remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1)) // cut one key from oldNode
}

// merge 2 nodes
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}
