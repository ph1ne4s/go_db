package btree

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

type KV struct {
	Path string
	fd   int
	tree BTree
}

func (db *KV) Open() error

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
	//4. fsync for persistence
	return syscall.Fsync(db.fd)
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
