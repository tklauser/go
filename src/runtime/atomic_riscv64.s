// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "textflag.h"

#define FENCE WORD $0x0ff0000f

// func publicationBarrier()
TEXT ·publicationBarrier(SB),NOSPLIT,$-8-0
	FENCE
	RET
