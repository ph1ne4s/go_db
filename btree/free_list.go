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
}
func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

// make the newly added items available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}
