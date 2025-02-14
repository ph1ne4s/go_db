package btree

// |	node1	|    |	node2	|	  |node3     |
// +-----------++-----------++-----------+
// | total=xxx|	 |			|	     |			 |
// | next=yyy | ==> | next=qqq | ==> | next=eee | ==> ...
// | size=zzz |	 | size=ppp |	     | size=rrr |
// | pointers |	 | pointers |	     | pointers |

// The node format:
// | type | size | total | next | pointers |
// | 2B   |  2B  |  8B   |  8B  | size * 8B|

type LNode []byte

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4+8+8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

func flnSize(node BNode) int
func flnNext(node BNode) uint64
func flnPtr(node BNode, idx int) uint64
func flnSetPtr(node BNode, idx int, ptr uint64)
func flnSetHeader(node BNode, size uint16, next uint64)
func flnSetTotal(node BNode, total uint64)

type FreeList struct {
	head uint64 //head pointer

	//callbacks for managing on-disk pages
	get func(uint64) BNode //derefernece a pointer
	new func(BNode) uint64 //append a new page
	use func(uint64,BNode) //reuse a page
}

func(fl *FreeList) Get(topn int) uint64{
	assert(0<=topn && topn<fl.Total(), "freelist top out of range")
	node:=fl.get(fl.head)
	for flnSize(node)<=topn{
		topn-=flnSize(node)
		next:=flnNext(node)
		assert(next!=0, "last node")
	}
	return flnPtr(node, flnSize(node)-topn-1)
}