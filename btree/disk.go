package btree

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

type KV struct {
	Path   string
	fd     int
	tree   BTree
	failed bool // Did the last update fail?
	free   FreeList

	mmap struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64            // database size in number of pages
		temp    [][]byte          // newly allocated pages
		nfree int //number of pages taken from free list
		nappend uint64            // number of pages to be appended
		updates map[uint64][]byte // pending updates, including appended pages
	}
}
// callback for BTree & FreeList, dereference a pointer.
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
	assert(page != nil, " page nil")
	return BNode(page) // for new pages
	}
	return pageGetMapped(db, ptr) // for written pages
	}

func pageGetMapped(db *KV, ptr uint64) BNode {
		start := uint64(0)
		for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode(chunk[offset : offset+BTREE_PAGE_SIZE])
			}
			start = end
			}
			panic("bad ptr")
			}
// callback for BTree, allocate a new page.
func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node) <= BTREE_PAGE_SIZE, "node size too big")
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
	// reuse a deallocated page
	ptr = db.free.Get(db.page.nfree)
	db.page.nfree++
	} else {
	// append a new page
	ptr = db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	}
	db.page.updates[ptr] = node
	return ptr
	}

	// callback for BTree, deallocate a page.
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
	}
// callback for FreeList, reuse a page.
func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node
	}

func createFileSync(file string) (int, error) {
	// get dir fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open dir: %w", err)
	}
	defer syscall.Close(dirfd) //defer runs just before func returns

	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	// fsync the dir
	if err = syscall.Fsync(dirfd); err != nil {
		_ = syscall.Close(fd) //may leave an empty file
		return -1, fmt.Errorf("fsync dir: %w", err)
	}
	return fd, nil

}
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<20) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}
// callback for FreeList, allocate a new page.
func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node) <= BTREE_PAGE_SIZE, "node too big")
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node
	return ptr
	}

	func writePages(db *KV) error {
		// update the free list
		freed := []uint64{}
		for ptr, page := range db.page.updates {
		if page == nil {
		freed = append(freed, ptr)
		}
		}
		db.free.Update(db.page.nfree, freed)
		// extend the file & mmap if needed
		// omitted...
		// copy pages to the file
		for ptr, page := range db.page.updates {
		if page != nil {
		copy(pageGetMapped(db, ptr), page)
		}
	}
	return nil
	}

