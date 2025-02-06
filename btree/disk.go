package btree

type KV struct {
	Path string
	fd   int
	tree BTree
}

func (db *KV) Open() error
