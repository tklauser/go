// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sys

const (
	ArchFamily          = RISCV64
	BigEndian           = false
	DefaultPhysPageSize = 4096
	PCQuantum           = 4
	Int64Align          = 8
	MinFrameSize        = 8
)

type Uintreg uint64
