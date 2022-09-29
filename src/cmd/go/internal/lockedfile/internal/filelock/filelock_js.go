// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build js && wasm

package filelock

import (
	"errors"
	"io/fs"
	"sync"
	"syscall"
)

type lockType int8

const (
	readLock = iota + 1
	writeLock
)

type inode = uint64 // type of syscall.Stat_t.Ino

type fileLock struct {
	file File
	typ  lockType
}

type inodeLock struct {
	owner fileLock
	queue []<-chan fileLock
}

var (
	mu     sync.Mutex
	inodes = map[File]inode{}
	locks  = map[inode]inodeLock{}
)

func lock(f File, lt lockType) error {
	// POSIX locks apply per inode and process, and the lock for an inode is
	// released when *any* descriptor for that inode is closed. So we need to
	// synchronize access to each inode internally, and must serialize lock and
	// unlock calls that refer to the same inode through different descriptors.
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	ino := fi.Sys().(*syscall.Stat_t).Ino

	mu.Lock()
	if i, dup := inodes[f]; dup && i != ino {
		mu.Unlock()
		return &fs.PathError{
			Op:   lt.String(),
			Path: f.Name(),
			Err:  errors.New("inode for file changed since last Lock or RLock"),
		}
	}
	inodes[f] = ino

	fl := fileLock{file: f, typ: lt}
	var wait chan File
	l := locks[ino]
	if l.owner.file == f {
		// This file already owns the lock, but the call may change its lock type.
		if l.owner.typ != lt {
			return &fs.PathError{
				Op:   lt.String(),
				Path: f.Name(),
				Err:  errors.New("lock type for file changed"),
			}
		}
	} else if l.owner == nil {
		// No owner: it's ours now.
		l.owner = fl
	} else {
		// Already owned: add a channel to wait on.
		wait = make(chan fileLock)
		l.queue = append(l.queue, wait)
	}
	locks[ino] = l
	mu.Unlock()

	if wait != nil {
		wait <- fl
	}

	return nil
}

func unlock(f File) error {
	var owner File

	mu.Lock()
	ino, ok := inodes[f]
	if ok {
		owner = locks[ino].owner.file
	}
	mu.Unlock()

	if owner != f {
		panic("unlock called on a file that is not locked")
	}

	mu.Lock()
	l := locks[ino]
	if len(l.queue) == 0 {
		// No waiters: remove the map entry.
		delete(locks, ino)
	} else {
		// The first waiter is sending us their file now.
		// Receive it and update the queue.
		l.owner = <-l.queue[0]
		l.queue = l.queue[1:]
		locks[ino] = l
	}
	delete(inodes, f)
	mu.Unlock()

	return err
}

func isNotSupported(err error) bool {
	return err == ErrNotSupported
}
