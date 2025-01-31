package btree

import (
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
func (node BNode) getPtr(idx uint16) uint16 {
	assert(idx < node.nkeys(), "getptr")
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys(), "setptr")
	pos := 4 + 8*idx
	binary.BigEndian.PutUint64(node[pos:], val)
}

// read offset array
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
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
	return node.kvPos(node.nkeys())
}
