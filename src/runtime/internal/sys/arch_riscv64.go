// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sys

const (
	ArchFamily          = RISCV64
	BigEndian           = false
	CacheLineSize       = 64
	DefaultPhysPageSize = 4096
	PCQuantum           = 4
	Int64Align          = 8
	HugePageSize        = 1 << 21
	MinFrameSize        = 8
)

type Uintreg uint64
