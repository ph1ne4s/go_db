package btree

import (
	"bytes"
	"encoding/binary"
)

type Node struct {
	keys [][]byte

	vals [][]byte
	kids []*Node
}

type BNode []byte

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func assert(p bool, msg string) {
	if p {
		panic(msg)
	}
}
func init() {
	node1max := 4 + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max <= BTREE_PAGE_SIZE {
		panic("Size exceeded")
	}
}

// getters
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// setter
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// r/w child pointers array
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys(), "getptr")
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys(), "setptr")
	pos := 4 + 8*idx
	binary.BigEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BNode, idx uint16) uint16 {
	assert(idx < 1 || idx > node.nkeys(), "offsetPos: Index out of bounds!")

	return 4 + 8*node.nkeys() + 2*(idx-1)
}

// read offset array
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

// updates offset for kv-pair at given index
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys(), "kvpos")
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset((idx))
}
func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys(), "getkeys")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys(), "gatval")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen] //pos is location of kv pair, 4 is {2 for key len and 2 for val len}, klen is length of key and is later sliced till vlen to get only val
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)
	pos := new.kvPos(idx)

	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(key)+len(val))))
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys()) // uses the offset value of the last key
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy keys before idx
	nodeAppendKV(new, idx, 0, key, val)                    //new keys
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) //keys from idx
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// find the last postion that is less than or equal to the key
func nodeLookupLE(node BNode, key []byte) uint16 { // cna be binary search, rn linear
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

//For an in-memory B+tree, an oversized node can be split into 2 nodes, each with half of the keys. For a disk-based B+tree, half of the keys may not fit into a page due to uneven key sizes. However, we can use the half position as an initial guess, then move it left or right if the half is too large.

func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2, "nodesplit")
	nleft := old.nkeys() / 2
	leftbytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for leftbytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1, "nleft_split if less")
	rightbytes := func() uint16 {
		return old.nbytes() - leftbytes() + 4
	}
	for rightbytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nkeys(), "nleft too big")
	nright := old.nkeys() - nleft

	//new_nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	//if left too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE, "left still too big")
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} //not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, "node split 3")
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

type BTree struct {
	root uint16 //root pointer

	//callbacks for on disk pages
	get func(uint16) []byte //read data from page
	new func([]byte) uint16 // allocate a new page with data
	del func(uint16)        // deallocate a page number

}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), uint64(tree.new(node)), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
func nodeReplace2Kid(new BNode, old BNode, idx uint16, merged uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, merged, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+1))
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert
		}
	case BNODE_NODE:
		// recursive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(uint16(kptr)), key, val)
		// after insertion, split the result
		nsplit, split := nodeSplit3(knode)
		// deallocate the old kid node
		tree.del(uint16(kptr))
		// update the kid links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	}
	return new
}
