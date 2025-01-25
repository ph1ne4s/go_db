package main

import (
	"fmt"
	"math/rand"
	"os"
)

func SaveData(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Intn(1000))
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664) //permissions of file(write), and create if not exist

	if err != nil {
		return err
	}
	defer func() { //4-discard temp file if it exists(file fp is closed when func returns)
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err = fp.Write(data); err != nil { //1- save data in temp file
		return err
	}

	if err = fp.Sync(); err != nil { //2-fsync(memory to disk)
		return err
	}

	err = os.Rename(tmp, path) //replace org file
	return err

}
