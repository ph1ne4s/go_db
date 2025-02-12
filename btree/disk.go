package btree

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
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
		nappend uint64            // number of pages to be appended
		updates map[uint64][]byte // pending updates, including appended pages
	}
}

func updateFile(db *KV) error {
	//1.new node
	if err := writePages(db); err != nil {
		return err
	}
	//2.fsync for order in 1 and 3
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}
	//3.update root pointer atomically
	if err := updateRoot(db); err != nil {
		return err
	}
	// prepare the free list for the next update
	db.free.SetMaxSeq()
	return nil
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

// `BTree.get`, read a page.
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	return db.pageReadFile(ptr)
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
func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp)) // just append
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func writePages(db *KV) error {
	// extending mmap
	size := (int(db.page.flushed) + len(db.page.temp)) * BTREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}
	//write data pages to file
	offset := int64(db.page.flushed * BTREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}
	//discard in-memory data
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

const DB_SIG = "BuildingDB"

// sig  root_ptr  page_used
// 16B  8B         8B

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}
func loadMeta(db *KV, data []byte)

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 2 //meata page and a free list node
		// add an initial node to the free list so it's never empty
		db.free.headPage = 1 // the 2nd page
		db.free.tailPage = 1
		return nil
	}
	//read page
	data := db.mmap.chunks[0]
	loadMeta(db, data)

	return nil
}
func updateRoot(db *KV) error { //update meta atomically
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}
func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db) // save the in-memory state (tree root)
	if err := db.tree.Insert(key, val); err != nil {
		return err // length limit
	}
	return updateOrRevert(db, meta)
}
func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		// write and fsync the previous meta page
		// ...
		db.failed = false
	}
	// 2-phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		// the in-memory states can be reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.temp = db.page.temp[:0]
		db.failed = true
	}

	return err
}
