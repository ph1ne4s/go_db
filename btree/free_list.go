package btree

// node format:
// | next | pointers | unused |
// |  8B  |   n*8B   |   ...  |

// first_item
// ↓
// head_page -> [ next |    xxxxx ]
// ↓
// [ next | xxxxxxxx ]
// ↓
// tail_page -> [ NULL | xxxx     ]
// ↑
// last_item

type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// getters & setters
func (node LNode) getNext() uint64
func (node LNode) setNext(next uint64)
func (node LNode) getPtr(idx int) uint64
func (node LNode) setPtr(idx int, ptr uint64)

type FreeList struct {
	// callbacks for managing on-disk pages
	get func(uint64) []byte // read a page
	new func([]byte) uint64 // append a new page
	set func(uint64) []byte // update an existing page
	// persisted data in the meta page
	headPage uint64 // pointer to the list head node
	headSeq  uint64 // monotonic sequence number to index into the list head
	tailPage uint64
	tailSeq  uint64
	// in-memory states
	maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
}

func (db *KV) Open() error {
	//B+tree callbacks
	db.tree.get = db.pageRead      //read a page
	db.tree.new = db.pageAlloc     //reuse from freelist or append
	db.tree.del = db.free.PushTail //freed pages go to free lsit

	// free list callbacks
	db.free.get = db.pageRead   //read page
	db.free.new = db.pageAppend //append a page
	db.free.set = db.pageWrite  // in-place updates
	return nil
}
func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

// make the newly added items available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

func f1Pop(f1 *FreeList) (ptr uint64, head uint64) { //remove 1 item from headnode, remove node if empty
	if f1.headSeq == f1.maxSeq {
		return 0, 0 //cannot go further
	}

	node := LNode(f1.get(f1.headPage))
	ptr = node.getPtr(seq2idx(f1.headSeq)) //item ptr
	f1.headSeq++
	// move to next if head empty
	if seq2idx(f1.headSeq) == 0 {
		head, f1.headPage = f1.headPage, node.getNext()
		assert(f1.headPage != 0, "no nodes left :_)")
	}
	return // head will be 0 if the condition above is not met(deafult 0 for named variable)
}

// get 1 item from list head, else 0
func (f1 *FreeList) PopHead() uint64 {
	ptr, head := f1Pop(f1)
	if head != 0 { //empty head is recycled
		f1.PushTail(head)
	}
	return ptr
}

func (f1 *FreeList) PushTail(ptr uint64) { //add item to tail
	LNode(f1.set(f1.tailPage)).setPtr(seq2idx(f1.tailSeq), ptr) //add it to tail node
	f1.tailSeq++
	if seq2idx(f1.tailSeq) == 0 { //new tail node if full
		next, head := f1Pop(f1) //reuse from head, may remove head node
		if next == 0 {
			next = f1.new(make([]byte, BTREE_PAGE_SIZE))
		}
		//link to new tail node
		LNode(f1.set(f1.tailPage)).setNext(next)
		f1.tailPage = next
		if head != 0 {
			LNode(f1.set(f1.tailPage)).setPtr(0, head)
			f1.tailSeq++
		}

	}
}

// `BTree.new`, allocate a new page.
func (db *KV) pageAlloc(node []byte) uint64 {
	if ptr := db.free.PopHead(); ptr != 0 { // try the free list
		db.page.updates[ptr] = node
		return ptr
	}
	return db.pageAppend(node) // append
}

// `FreeList.set`, update an existing page.
func (db *KV) pageWrite(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	node := make([]byte, BTREE_PAGE_SIZE)
	copy(node, db.pageReadFile(ptr)) // initialized from the file
	db.page.updates[ptr] = node
	return node
}
func (db *KV) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}
