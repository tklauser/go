// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Like all Go assemblers, this assembler proceeds in four steps: progedit,
// follow, preprocess, and assemble.
//
// The Go assembler framework occasionally abuses certain fields in the Prog and
// Addr structs.  For instance, the instruction
//
//   JAL T1, label
//
// jumps to the address ZERO+label and stores a linkage pointer in T1.  Since
// ZERO is an input register and T1 is an output register, you might expect the
// assembler's parser to set From to be ZERO and To to be T1--but you'd be
// wrong!  Instead, From is T1 and To is ZERO.  Repairing this infelicity would
// require changes to the parser and every assembler backend, so until that
// cleanup occurs, the authors have tried to document specific gotchas where
// they occur.  Be on the lookout.

package riscv64

import (
	"cmd/internal/obj"
	"cmd/internal/objabi"
	"fmt"
)

// ctxtRiscv holds state while assembling a single function.
// Each function gets a fresh ctxtRiscv.
// This allows for multiple functions to be safely concurrently assembled.
type ctxtRiscv struct {
	ctxt       *obj.Link
	newprog    obj.ProgAlloc
	cursym     *obj.LSym
	autosize   int32
	instoffset int64
	pc         int64
}

// stackOffset updates Addr offsets based on the current stack size.
//
// The stack looks like:
// -------------------
// |                 |
// |      PARAMs     |
// |                 |
// |                 |
// -------------------
// |    Parent RA    |   SP on function entry
// -------------------
// |                 |
// |                 |
// |       AUTOs     |
// |                 |
// |                 |
// -------------------
// |        RA       |   SP during function execution
// -------------------
//
// FixedFrameSize makes other packages aware of the space allocated for RA.
//
// Slide 21 on the presention attached to
// https://golang.org/issue/16922#issuecomment-243748180 has a nicer version
// of this diagram.
func stackOffset(a *obj.Addr, stacksize int64) {
	switch a.Name {
	case obj.NAME_AUTO:
		// Adjust to the top of AUTOs.
		a.Offset += stacksize
	case obj.NAME_PARAM:
		// Adjust to the bottom of PARAMs.
		a.Offset += stacksize + 8
	}
}

// lowerjalr normalizes a JALR instruction.
func lowerjalr(p *obj.Prog) {
	if p.As != AJALR {
		panic("lowerjalr: not a JALR")
	}

	// JALR gets parsed like JAL--the linkage pointer goes in From, and the
	// target is in To.  However, we need to assemble it as an I-type
	// instruction--the linkage pointer will go in To, the target register
	// in From3, and the offset in From.
	//
	// TODO(bbaren): Handle sym, symkind, index, and scale.
	p.SetFrom3(p.To)
	p.From, p.To = p.To, p.From

	// Reset Reg so the string looks correct.
	p.From.Type = obj.TYPE_CONST
	p.From.Reg = obj.REG_NONE

	// Reset Offset so the string looks correct.
	p.GetFrom3().Type = obj.TYPE_REG
	p.GetFrom3().Offset = 0
}

// jalrToSym replaces p with a set of Progs needed to jump to the Sym in p.
//
// lr is the link register to use for the JALR.
//
// p must be a CALL or JMP.
func (c *ctxtRiscv) jalrToSym(p *obj.Prog, lr int16) *obj.Prog {
	if p.As != obj.ACALL && p.As != obj.AJMP {
		c.ctxt.Diag("unexpected Prog in jalrToSym: %v", p)
		return p
	}

	// AUIPC $off_hi, TMP
	// ADDI $off_lo, TMP
	// JALR lr, TMP
	to := p.To

	p.As = AAUIPC
	// This offset isn't really encoded with either instruction. It will be
	// extracted for a relocation later.
	p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: to.Offset, Sym: to.Sym}
	p.SetFrom3(obj.Addr{})
	p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
	p.Mark |= NEED_PCREL_ITYPE_RELOC
	p = obj.Appendp(p, c.newprog)

	p.As = AADDI
	p.From = obj.Addr{Type: obj.TYPE_CONST}
	p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP})
	p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
	p = obj.Appendp(p, c.newprog)

	p.As = AJALR
	p.From.Type = obj.TYPE_REG
	p.From.Reg = lr
	// Leave Sym only for the CALL reloc in assemble.
	p.From.Sym = to.Sym
	p.SetFrom3(obj.Addr{})
	p.To.Type = obj.TYPE_REG
	p.To.Reg = REG_TMP
	lowerjalr(p)

	return p
}

// movtol converts a MOV mnemonic into the corresponding load instruction.
func movtol(mnemonic obj.As) obj.As {
	switch mnemonic {
	case AMOV:
		return ALD
	case AMOVB:
		return ALB
	case AMOVH:
		return ALH
	case AMOVW:
		return ALW
	case AMOVBU:
		return ALBU
	case AMOVHU:
		return ALHU
	case AMOVWU:
		return ALWU
	case AMOVF:
		return AFLW
	case AMOVD:
		return AFLD
	default:
		panic(fmt.Sprintf("%+v is not a MOV", mnemonic))
	}
}

// movtos converts a MOV mnemonic into the corresponding store instruction.
func movtos(mnemonic obj.As) obj.As {
	switch mnemonic {
	case AMOV:
		return ASD
	case AMOVB:
		return ASB
	case AMOVH:
		return ASH
	case AMOVW:
		return ASW
	case AMOVF:
		return AFSW
	case AMOVD:
		return AFSD
	default:
		panic(fmt.Sprintf("%+v is not a MOV", mnemonic))
	}
}

// addrtoreg extracts the register from an Addr, handling special Addr.Names.
func addrtoreg(a obj.Addr) int16 {
	switch a.Name {
	case obj.NAME_PARAM, obj.NAME_AUTO:
		return REG_SP
	}
	return a.Reg
}

// progedit is called individually for each Prog.  It normalizes instruction
// formats and eliminates as many pseudoinstructions as it can.
func progedit(ctxt *obj.Link, p *obj.Prog, newprog obj.ProgAlloc) {
	// Ensure everything has a From3 to eliminate a ton of nil-pointer
	// checks later.
	if p.GetFrom3() == nil {
		p.SetFrom3(obj.Addr{Type: obj.TYPE_NONE})
	}

	// Expand binary instructions to ternary ones.
	if p.GetFrom3().Type == obj.TYPE_NONE {
		switch p.As {
		case AADD, ASUB, ASLL, AXOR, ASRL, ASRA, AOR, AAND, AMUL, AMULH,
			AMULHU, AMULHSU, AMULW, ADIV, ADIVU, AREM, AREMU, ADIVW,
			ADIVUW, AREMW, AREMUW:
			p.GetFrom3().Type = obj.TYPE_REG
			p.GetFrom3().Reg = p.To.Reg
		}
	}

	// Rewrite instructions with constant operands to refer to the immediate
	// form of the instruction.
	if p.From.Type == obj.TYPE_CONST {
		switch p.As {
		case AADD:
			p.As = AADDI
		case AAND:
			p.As = AANDI
		case AOR:
			p.As = AORI
		case ASLL:
			p.As = ASLLI
		case ASLT:
			p.As = ASLTI
		case ASLTU:
			p.As = ASLTIU
		case ASRA:
			p.As = ASRAI
		case ASRL:
			p.As = ASRLI
		case AXOR:
			p.As = AXORI
		}
	}

	// Do additional single-instruction rewriting.
	switch p.As {
	// Turn JMP into JAL ZERO or JALR ZERO.
	case obj.AJMP:
		// p.From is actually an _output_ for this instruction.
		p.From.Type = obj.TYPE_REG
		p.From.Reg = REG_ZERO

		switch p.To.Type {
		case obj.TYPE_BRANCH:
			p.As = AJAL
		case obj.TYPE_MEM:
			switch p.To.Name {
			case obj.NAME_AUTO, obj.NAME_PARAM, obj.NAME_NONE:
				p.As = AJALR
				lowerjalr(p)
			case obj.NAME_EXTERN:
				// Handled in preprocess.
			default:
				ctxt.Diag("progedit: unsupported name %d for %v", p.To.Name, p)
			}
		default:
			panic(fmt.Sprintf("unhandled type %+v", p.To.Type))
		}

	case obj.ACALL:
		switch p.To.Type {
		case obj.TYPE_MEM:
			// Handled in preprocess.
		case obj.TYPE_REG:
			p.As = AJALR
			p.From.Type = obj.TYPE_REG
			p.From.Reg = REG_RA
			lowerjalr(p)
		default:
			ctxt.Diag("unknown destination type %+v in CALL: %v", p.To.Type, p)
		}

	case AJALR:
		lowerjalr(p)

	case obj.AUNDEF, AECALL, AEBREAK, ASCALL, ARDCYCLE, ARDTIME, ARDINSTRET:
		if p.As == obj.AUNDEF {
			p.As = AEBREAK
		}
		// SCALL is the old name for ECALL.
		if p.As == ASCALL {
			p.As = AECALL
		}

		i, ok := encode(p.As)
		if !ok {
			panic("progedit: tried to rewrite nonexistent instruction")
		}
		p.From.Type = obj.TYPE_CONST
		// The CSR isn't exactly an offset, but it winds up in the
		// immediate area of the encoded instruction, so record it in
		// the Offset field.
		p.From.Offset = i.csr
		p.GetFrom3().Type = obj.TYPE_REG
		p.GetFrom3().Reg = REG_ZERO
		if p.To.Type == obj.TYPE_NONE {
			p.To.Type = obj.TYPE_REG
			p.To.Reg = REG_ZERO
		}

	case ASEQZ:
		// SEQZ rs, rd -> SLTIU $1, rs, rd
		p.As = ASLTIU
		p.SetFrom3(p.From)
		p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: 1}

	case ASNEZ:
		// SNEZ rs, rd -> SLTU rs, x0, rd
		p.As = ASLTU
		p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_ZERO})

	// For binary float instructions, use From3 and To, not From and
	// To. This helps simplify encoding.
	case AFNEGS:
		// FNEGS rs, rd -> FSGNJNS rs, rs, rd
		p.As = AFSGNJNS
		p.SetFrom3(p.From)
	case AFNEGD:
		// FNEGD rs, rd -> FSGNJND rs, rs, rd
		p.As = AFSGNJND
		p.SetFrom3(p.From)
	case AFSQRTS, AFSQRTD:
		p.SetFrom3(p.From)

		// This instruction expects a zero (i.e., float register 0) to
		// be the second input operand.
		p.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_F0}
	case AFCVTWS, AFCVTLS, AFCVTWUS, AFCVTLUS, AFCVTWD, AFCVTLD, AFCVTWUD, AFCVTLUD:
		// Set the rounding mode in funct3 to round to zero
		p.Scond = 1
	}
}

// follow can do some optimization on the structure of the program.  Currently,
// follow does nothing.
func follow(ctxt *obj.Link, s *obj.LSym) {}

// setpcs sets the Pc field in all instructions reachable from p.  It uses pc as
// the initial value.
func setpcs(p *obj.Prog, pc int64) {
	for ; p != nil; p = p.Link {
		p.Pc = pc
		pc += encodingForP(p).length
	}
}

// InvertBranch inverts the condition of a conditional branch.
func InvertBranch(i obj.As) obj.As {
	switch i {
	case ABEQ:
		return ABNE
	case ABNE:
		return ABEQ
	case ABLT:
		return ABGE
	case ABGE:
		return ABLT
	case ABLTU:
		return ABGEU
	case ABGEU:
		return ABLTU
	default:
		panic("InvertBranch: not a branch")
	}
}

// containsCall reports whether the symbol contains a CALL (or equivalent)
// instruction. Must be called after progedit.
func containsCall(sym *obj.LSym) bool {
	// CALLs are CALL or JAL(R) with link register RA.
	for p := sym.Func.Text; p != nil; p = p.Link {
		switch p.As {
		case obj.ACALL:
			return true
		case AJAL, AJALR:
			if p.To.Type == obj.TYPE_REG && p.To.Reg == REG_RA {
				return true
			}
		}
	}

	return false
}

// loadImmIntoRegTmp loads the immediate (low, high), generated by Split32BitImmediate into REG_TMP.
//
// The following instruction sequence is generated:
// LUI $high, TMP
// ADDI $low, TMP, TMP
//
// p is overwritten with LUI and the Prog returned is an empty Prog following ADDI.
func loadImmIntoRegTmp(p *obj.Prog, newprog obj.ProgAlloc, low, high int64) *obj.Prog {
	p.As = ALUI
	p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: high}
	p.RestArgs = nil
	p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
	p.Spadj = 0 // needed if TO is SP
	p = obj.Appendp(p, newprog)

	p.As = AADDIW
	p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: low}
	p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP})
	p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
	p = obj.Appendp(p, newprog)

	return p
}

// preprocess generates prologue and epilogue code, computes PC-relative branch
// and jump offsets, and resolves psuedo-registers.
//
// preprocess is called once per linker symbol.
//
// When preprocess finishes, all instructions in the symbol are either
// concrete, real RISC-V instructions or directive pseudo-ops like TEXT,
// PCDATA, and FUNCDATA.
func preprocess(ctxt *obj.Link, cursym *obj.LSym, newprog obj.ProgAlloc) {
	// Generate the prologue.
	if cursym.Func.Text.As != obj.ATEXT {
		ctxt.Diag("preprocess: found symbol that does not start with TEXT directive")
		return
	}

	c := ctxtRiscv{ctxt: ctxt, newprog: newprog, cursym: cursym}

	p := c.cursym.Func.Text
	stacksize := p.To.Offset

	if stacksize < 0 {
		p.GetFrom3().Offset |= obj.NOFRAME
		stacksize = 0
	}
	// We must save RA if there is a CALL.
	saveRA := containsCall(cursym)
	// Unless we're told not to!
	if p.GetFrom3().Offset&obj.NOFRAME != 0 {
		saveRA = false
	}
	if saveRA {
		stacksize += 8
	}

	c.cursym.Func.Args = p.To.Val.(int32)
	c.cursym.Func.Locals = int32(stacksize)

	prologue := p

	if p.GetFrom3().Offset&obj.NOSPLIT == 0 {
		prologue = c.stacksplit(prologue, stacksize) // emit split check
	}

	// Insert stack adjustment if necessary.
	if stacksize != 0 {
		prologue = obj.Appendp(prologue, newprog)
		prologue.As = AADDI
		prologue.From.Type = obj.TYPE_CONST
		prologue.From.Offset = -stacksize
		prologue.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_SP})
		prologue.To.Type = obj.TYPE_REG
		prologue.To.Reg = REG_SP
		prologue.Spadj = int32(stacksize)
	}

	// Actually save RA.
	if saveRA {
		// Source register in From3, destination base register in To,
		// destination offset in From. See MOV TYPE_REG, TYPE_MEM below
		// for details.
		prologue = obj.Appendp(prologue, newprog)
		prologue.As = ASD
		prologue.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_RA})
		prologue.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_SP}
		prologue.From = obj.Addr{Type: obj.TYPE_CONST, Offset: 0}
	}

	if cursym.Func.Text.GetFrom3().Offset&obj.WRAPPER != 0 {
		// if(g->panic != nil && g->panic->argp == FP) g->panic->argp = bottom-of-frame
		//
		//   MOV g_panic(g), A1
		//   BNE A1, ZERO, adjust
		// end:
		//   NOP
		// ...rest of function..
		// adjust:
		//   MOV panic_argp(A1), A2
		//   ADD $(autosize+FIXED_FRAME), X2, A3
		//   BNE A2, A3, end
		//   ADD $FIXED_FRAME, X2, A2
		//   MOV A2, panic_argp(A1)
		//   JMP end
		//
		// The NOP is needed to give the jumps somewhere to land.
		// It is a liblink NOP, not an mips NOP: it encodes to 0 instruction bytes.

		ldpanic := obj.Appendp(prologue, newprog)

		ldpanic.As = AMOV
		ldpanic.From = obj.Addr{Type: obj.TYPE_MEM, Reg: REGG, Offset: 4 * int64(ctxt.Arch.PtrSize)} // G.panic
		ldpanic.SetFrom3(obj.Addr{})
		ldpanic.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A1}

		bneadj := obj.Appendp(ldpanic, newprog)
		bneadj.As = ABNE
		bneadj.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A1}
		bneadj.Reg = REG_ZERO
		bneadj.To.Type = obj.TYPE_BRANCH

		endadj := obj.Appendp(bneadj, newprog)
		endadj.As = obj.ANOP

		last := endadj
		for last.Link != nil {
			last = last.Link
		}

		getargp := obj.Appendp(last, newprog)
		getargp.As = AMOV
		getargp.From = obj.Addr{Type: obj.TYPE_MEM, Reg: REG_A1, Offset: 0} // Panic.argp
		getargp.SetFrom3(obj.Addr{})
		getargp.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A2}

		bneadj.Pcond = getargp

		calcargp := obj.Appendp(getargp, newprog)
		calcargp.As = AADDI
		calcargp.From = obj.Addr{Type: obj.TYPE_CONST, Offset: stacksize + ctxt.FixedFrameSize()}
		calcargp.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_SP})
		calcargp.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A3}

		testargp := obj.Appendp(calcargp, newprog)
		testargp.As = ABNE
		testargp.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A2}
		testargp.Reg = REG_A3
		testargp.To.Type = obj.TYPE_BRANCH
		testargp.Pcond = endadj

		adjargp := obj.Appendp(testargp, newprog)
		adjargp.As = AADDI
		adjargp.From = obj.Addr{Type: obj.TYPE_CONST, Offset: int64(ctxt.Arch.PtrSize)}
		adjargp.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_X2})
		adjargp.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A2}

		setargp := obj.Appendp(adjargp, newprog)
		setargp.As = AMOV
		setargp.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_A2}
		setargp.SetFrom3(obj.Addr{})
		setargp.To = obj.Addr{Type: obj.TYPE_MEM, Reg: REG_A1, Offset: 0} // Panic.argp

		godone := obj.Appendp(setargp, newprog)
		godone.As = AJAL
		godone.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_ZERO}
		godone.To.Type = obj.TYPE_BRANCH
		godone.Pcond = endadj
	}

	// Update stack-based offsets.
	for p := cursym.Func.Text; p != nil; p = p.Link {
		stackOffset(&p.From, stacksize)
		if p.GetFrom3() != nil {
			stackOffset(p.GetFrom3(), stacksize)
		}
		stackOffset(&p.To, stacksize)

		// TODO: update stacksize when instructions that modify SP are
		// found, or disallow it entirely.
	}

	// Additional instruction rewriting. Any rewrites that change the number
	// of instructions must occur here (i.e., before jump target
	// resolution).
	for p := cursym.Func.Text; p != nil; p = p.Link {
		switch p.As {

		// Rewrite MOV. This couldn't be done in progedit, as SP
		// offsets needed to be applied before we split up some of the
		// Addrs.
		case AMOV, AMOVB, AMOVH, AMOVW, AMOVBU, AMOVHU, AMOVWU, AMOVF, AMOVD:
			switch p.From.Type {
			case obj.TYPE_MEM: // MOV c(Rs), Rd -> L $c, Rs, Rd
				switch p.From.Name {
				case obj.NAME_AUTO, obj.NAME_PARAM, obj.NAME_NONE:
					if p.To.Type != obj.TYPE_REG {
						ctxt.Diag("progedit: unsupported load at %v", p)
					}
					p.As = movtol(p.As)
					p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: addrtoreg(p.From)})
					p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: p.From.Offset}
				case obj.NAME_EXTERN, obj.NAME_STATIC:
					// AUIPC $off_hi, R
					// L $off_lo, R
					as := p.As
					to := p.To

					p.As = AAUIPC
					// This offset isn't really encoded
					// with either instruction. It will be
					// extracted for a relocation later.
					p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: p.From.Offset, Sym: p.From.Sym}
					p.SetFrom3(obj.Addr{})
					p.To = obj.Addr{Type: obj.TYPE_REG, Reg: to.Reg}
					p.Mark |= NEED_PCREL_ITYPE_RELOC
					p = obj.Appendp(p, newprog)

					p.As = movtol(as)
					p.From = obj.Addr{Type: obj.TYPE_CONST}
					p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: to.Reg})
					p.To = to
				default:
					ctxt.Diag("progedit: unsupported name %d for %v", p.From.Name, p)
				}
			case obj.TYPE_REG:
				switch p.To.Type {
				case obj.TYPE_REG:
					switch p.As {
					case AMOV: // MOV Ra, Rb -> ADDI $0, Ra, Rb
						p.As = AADDI
						p.SetFrom3(p.From)
						p.From = obj.Addr{Type: obj.TYPE_CONST}
					case AMOVF: // MOVF Ra, Rb -> FSGNJS Ra, Ra, Rb
						p.As = AFSGNJS
						p.SetFrom3(p.From)
					case AMOVD: // MOVD Ra, Rb -> FSGNJD Ra, Ra, Rb
						p.As = AFSGNJD
						p.SetFrom3(p.From)
					default:
						ctxt.Diag("progedit: unsupported register-register move at %v", p)
					}
				case obj.TYPE_MEM: // MOV Rs, c(Rd) -> S $c, Rs, Rd
					switch p.As {
					case AMOVBU, AMOVHU, AMOVWU:
						ctxt.Diag("progedit: unsupported unsigned store at %v", p)
					}
					switch p.To.Name {
					case obj.NAME_AUTO, obj.NAME_PARAM, obj.NAME_NONE:
						p.As = movtos(p.As)
						// The destination address goes in p.From and
						// p.To here, with the offset in p.From and the
						// register in p.To. The source register goes in
						// p.From3.
						p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: p.From.Offset}
						p.SetFrom3(p.To)
						p.GetFrom3().Type = obj.TYPE_REG
						p.To = obj.Addr{Type: obj.TYPE_REG, Reg: addrtoreg(p.To)}
					case obj.NAME_EXTERN:
						// AUIPC $off_hi, TMP
						// S $off_lo, TMP, R
						as := p.As
						from := p.From

						p.As = AAUIPC
						// This offset isn't really encoded
						// with either instruction. It will be
						// extracted for a relocation later.
						p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: p.To.Offset, Sym: p.To.Sym}
						p.SetFrom3(obj.Addr{})
						p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
						p.Mark |= NEED_PCREL_STYPE_RELOC
						p = obj.Appendp(p, newprog)

						p.As = movtos(as)
						p.From = obj.Addr{Type: obj.TYPE_CONST}
						p.SetFrom3(from)
						p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
					default:
						ctxt.Diag("progedit: unsupported name %d for %v", p.From.Name, p)
					}
				default:
					ctxt.Diag("progedit: unsupported MOV at %v", p)
				}
			case obj.TYPE_CONST:
				// MOV $c, R
				// If c is small enough, convert to:
				//   ADD $c, ZERO, R
				// If not, convert to:
				//   LUI top20bits(c), R
				//   ADD bottom12bits(c), R, R
				if p.As != AMOV {
					ctxt.Diag("progedit: unsupported constant load at %v", p)
				}
				off := p.From.Offset
				to := p.To

				low, high, err := Split32BitImmediate(off)
				if err != nil {
					// TODO: use a constant pool for 64 bit constants?
					//
					// Or remove REG_TMP from the general purposes registers used by the compiler
					// and emulate riscv.rules, using REG_TMP as the 32 bit value staging ground?
					ctxt.Diag("%v: constant %d too large; see riscv.rules MOVQconst for how to make a 64 bit constant: %v", p, off, err)
				}

				// LUI is only necessary if the offset doesn't fit in 12-bits.
				needLUI := high != 0
				if needLUI {
					p.As = ALUI
					p.To = to
					// Pass top 20 bits to LUI.
					p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: high}
					p = obj.Appendp(p, newprog)
				}
				p.As = AADDIW
				p.To = to
				p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: low}
				p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_ZERO})
				if needLUI {
					p.GetFrom3().Reg = to.Reg
				}

			case obj.TYPE_ADDR: // MOV $sym+off(SP/SB), R
				if p.To.Type != obj.TYPE_REG || p.As != AMOV {
					ctxt.Diag("progedit: unsupported addr MOV at %v", p)
				}
				switch p.From.Name {
				case obj.NAME_EXTERN, obj.NAME_STATIC:
					// AUIPC $off_hi, R
					// ADDI $off_lo, R
					to := p.To

					p.As = AAUIPC
					// This offset isn't really encoded
					// with either instruction. It will be
					// extracted for a relocation later.
					p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: p.From.Offset, Sym: p.From.Sym}
					p.SetFrom3(obj.Addr{})
					p.To = to
					p.Mark |= NEED_PCREL_ITYPE_RELOC
					p = obj.Appendp(p, newprog)

					p.As = AADDI
					p.From = obj.Addr{Type: obj.TYPE_CONST}
					p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: to.Reg})
					p.To = to
				case obj.NAME_PARAM, obj.NAME_AUTO:
					p.As = AADDI
					p.GetFrom3().Type = obj.TYPE_REG
					p.From.Type = obj.TYPE_CONST
					p.GetFrom3().Reg = REG_SP
				case obj.NAME_NONE:
					p.As = AADDI
					p.GetFrom3().Type = obj.TYPE_REG
					p.GetFrom3().Reg = p.From.Reg
					p.From.Type = obj.TYPE_CONST
					p.From.Reg = 0
				default:
					ctxt.Diag("progedit: bad addr MOV from name %v at %v", p.From.Name, p)
				}
			default:
				ctxt.Diag("progedit: unsupported MOV at %v", p)
			}

		case obj.ACALL:
			switch p.To.Type {
			case obj.TYPE_MEM:
				c.jalrToSym(p, REG_RA)
			}

		case obj.AJMP:
			switch p.To.Type {
			case obj.TYPE_MEM:
				switch p.To.Name {
				case obj.NAME_EXTERN:
					// JMP to symbol.
					c.jalrToSym(p, REG_ZERO)
				}
			}

		// Replace RET with epilogue.
		case obj.ARET:
			if saveRA {
				// Restore RA.
				p.As = ALD
				p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_SP})
				p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: 0}
				p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_RA}
				p = obj.Appendp(p, newprog)
			}

			if stacksize != 0 {
				p.As = AADDI
				p.From.Type = obj.TYPE_CONST
				p.From.Offset = stacksize
				p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_SP})
				p.To.Type = obj.TYPE_REG
				p.To.Reg = REG_SP
				p.Spadj = int32(-stacksize)
				p = obj.Appendp(p, newprog)
			}

			p.As = AJALR
			p.From.Type = obj.TYPE_CONST
			p.From.Offset = 0
			p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_RA})
			p.To.Type = obj.TYPE_REG
			p.To.Reg = REG_ZERO
			// "Add back" the stack removed in the previous instruction.
			//
			// This is to avoid confusing pctospadj, which sums
			// Spadj from function entry to each PC, and shouldn't
			// count adjustments from earlier epilogues, since they
			// won't affect later PCs.
			p.Spadj = int32(stacksize)

		// Replace FNE[SD] with FEQ[SD] and NOT.
		case AFNES:
			if p.To.Type != obj.TYPE_REG {
				ctxt.Diag("progedit: FNES needs an integer register output")
			}
			dst := p.To.Reg
			p.As = AFEQS
			p := obj.Appendp(p, newprog)
			p.As = AXORI // [bit] xor 1 = not [bit]
			p.From.Type = obj.TYPE_CONST
			p.From.Offset = 1
			p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: dst})
			p.To.Type = obj.TYPE_REG
			p.To.Reg = dst
		case AFNED:
			if p.To.Type != obj.TYPE_REG {
				ctxt.Diag("progedit: FNED needs an integer register output")
			}
			dst := p.To.Reg
			p.As = AFEQD
			p := obj.Appendp(p, newprog)
			p.As = AXORI
			p.From.Type = obj.TYPE_CONST
			p.From.Offset = 1
			p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: dst})
			p.To.Type = obj.TYPE_REG
			p.To.Reg = dst
		}
	}

	// Split immediates larger than 12-bits
	for p := cursym.Func.Text; p != nil; p = p.Link {
		switch p.As {
		// <opi> $imm, FROM3, TO
		case AADDI, AANDI, AORI, AXORI:
			// LUI $high, TMP
			// ADDI $low, TMP, TMP
			// <op> TMP, FROM3, TO
			q := *p
			low, high, err := Split32BitImmediate(p.From.Offset)
			if err != nil {
				ctxt.Diag("%v: constant %d too large", p, p.From.Offset, err)
			}
			if high == 0 {
				break // no need to split
			}
			p = loadImmIntoRegTmp(p, newprog, low, high)

			switch q.As {
			case AADDI:
				p.As = AADD
			case AANDI:
				p.As = AAND
			case AORI:
				p.As = AOR
			case AXORI:
				p.As = AXOR
			default:
				ctxt.Diag("progedit: unsupported inst %v for splitting", q)
			}
			p.Spadj = q.Spadj
			p.To = q.To
			p.SetFrom3(*q.GetFrom3())
			p.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}

		// <load> $imm, FROM3, TO (load $imm+(FROM3), TO)
		// <store> $imm, FROM3, TO (store $imm+(TO), FROM3)
		case ALD, ALB, ALH, ALW, ALBU, ALHU, ALWU,
			ASD, ASB, ASH, ASW:
			// LUI $high, TMP
			// ADDI $low, TMP, TMP
			q := *p
			low, high, err := Split32BitImmediate(p.From.Offset)
			if err != nil {
				ctxt.Diag("%v: constant %d too large", p, p.From.Offset)
			}
			if high == 0 {
				break // no need to split
			}
			p = loadImmIntoRegTmp(p, newprog, low, high)

			switch q.As {
			case ALD, ALB, ALH, ALW, ALBU, ALHU, ALWU:
				// ADD TMP, FROM3, TMP
				// <load> $0, TMP, TO
				p.As = AADD
				p.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
				p.SetFrom3(*q.GetFrom3())
				p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
				p = obj.Appendp(p, newprog)

				p.As = q.As
				p.To = q.To
				p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: 0}
				p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP})
			case ASD, ASB, ASH, ASW:
				// ADD TMP, TO, TMP
				// <store> $0, FROM3, TMP
				p.As = AADD
				p.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
				p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: q.To.Reg})
				p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
				p = obj.Appendp(p, newprog)

				p.As = q.As
				p.SetFrom3(*q.GetFrom3())
				p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}
				p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: 0}
			}
		}
	}

	// Compute instruction addresses.  Once we do that, we need to check for
	// overextended jumps and branches.  Within each iteration, Pc differences
	// are always lower bounds (since the program gets monotonically longer,
	// a fixed point will be reached).  No attempt to handle functions > 2GiB.
	for {
		rescan := false
		setpcs(cursym.Func.Text, 0)
		for p := cursym.Func.Text; p != nil; p = p.Link {
			switch p.As {
			case ABEQ, ABNE, ABLT, ABGE, ABLTU, ABGEU:
				if p.To.Type != obj.TYPE_BRANCH {
					panic("assemble: instruction with branch-like opcode lacks destination")
				}
				offset := p.Pcond.Pc - p.Pc
				if offset < -4096 || 4096 <= offset {
					// Branch is long.  Replace it with a jump.
					jmp := obj.Appendp(p, newprog)
					jmp.As = AJAL
					jmp.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_ZERO}
					jmp.To = obj.Addr{Type: obj.TYPE_BRANCH}
					jmp.Pcond = p.Pcond

					p.As = InvertBranch(p.As)
					p.Pcond = jmp.Link
					// We may have made previous branches too long,
					// so recheck them.
					rescan = true
				}
			case AJAL:
				if p.Pcond == nil {
					panic("intersymbol jumps should be expressed as AUIPC+JALR")
				}
				offset := p.Pcond.Pc - p.Pc
				if offset < -(1<<20) || (1<<20) <= offset {
					// Replace with 2-instruction sequence
					jmp := obj.Appendp(p, newprog)
					jmp.As = AJALR
					jmp.From = obj.Addr{Type: obj.TYPE_CONST, Offset: 0}
					jmp.To = p.From
					jmp.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP})
					// Assuming TMP is not live across J instructions, since it's reserved by SSA that should be OK

					p.As = AAUIPC
					p.From = obj.Addr{Type: obj.TYPE_BRANCH} // not generally valid, fixed up in the next loop
					p.SetFrom3(obj.Addr{})
					p.To = obj.Addr{Type: obj.TYPE_REG, Reg: REG_TMP}

					rescan = true
				}
			}
		}

		if !rescan {
			break
		}
	}

	// Now that there are no long branches, resolve branch and jump targets.
	// At this point, instruction rewriting which changes the number of
	// instructions will break everything--don't do it!
	for p := cursym.Func.Text; p != nil; p = p.Link {
		switch p.As {
		case ABEQ, ABNE, ABLT, ABGE, ABLTU, ABGEU, AJAL:
			switch p.To.Type {
			case obj.TYPE_BRANCH:
				p.To.Type = obj.TYPE_CONST
				p.To.Offset = p.Pcond.Pc - p.Pc
			case obj.TYPE_MEM:
				panic("unhandled type")
			}
		case AAUIPC:
			if p.From.Type == obj.TYPE_BRANCH {
				low, high, err := Split32BitImmediate(p.Pcond.Pc - p.Pc)
				if err != nil {
					ctxt.Diag("%v: jump displacement %d too large", p, p.Pcond.Pc-p.Pc)
				}
				p.From = obj.Addr{Type: obj.TYPE_CONST, Offset: high}
				p.Link.From.Offset = low
			}
		}
	}

	// Validate all instructions. This provides nice error messages.
	for p := cursym.Func.Text; p != nil; p = p.Link {
		encodingForP(p).validate(p)
	}
}

func (c *ctxtRiscv) stacksplit(p *obj.Prog, framesize int64) *obj.Prog {
	// Leaf function with no frame is effectively NOSPLIT.
	if framesize == 0 {
		return p
	}

	// MOV	g_stackguard(g), A0
	p = obj.Appendp(p, c.newprog)
	p.As = AMOV
	p.From.Type = obj.TYPE_MEM
	p.From.Reg = REGG
	p.From.Offset = 2 * int64(c.ctxt.Arch.PtrSize) // G.stackguard0
	if c.cursym.CFunc() {
		p.From.Offset = 3 * int64(c.ctxt.Arch.PtrSize) // G.stackguard1
	}
	p.To.Type = obj.TYPE_REG
	p.To.Reg = REG_A0

	var to_done, to_more *obj.Prog

	if framesize <= objabi.StackSmall {
		// small stack: SP < stackguard
		//	BGTU	SP, stackguard, done
		p = obj.Appendp(p, c.newprog)
		p.As = ABLTU
		p.From.Type = obj.TYPE_REG
		p.From.Reg = REG_A0
		p.Reg = REG_X2
		p.To.Type = obj.TYPE_BRANCH
		to_done = p
	} else if framesize <= objabi.StackBig {
		// large stack: SP-framesize < stackguard-StackSmall
		//	ADD	$-framesize, SP, A1
		//	BGTU	A1, stackguard, done
		p = obj.Appendp(p, c.newprog)
		// TODO(sorear): logic inconsistent with comment, but both match all non-x86 arches
		p.As = AADDI
		p.From.Type = obj.TYPE_CONST
		p.From.Offset = int64(-framesize)
		p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_X2})
		p.To.Type = obj.TYPE_REG
		p.To.Reg = REG_A1

		p = obj.Appendp(p, c.newprog)
		p.As = ABLTU
		p.From.Type = obj.TYPE_REG
		p.From.Reg = REG_A0
		p.Reg = REG_A1
		p.To.Type = obj.TYPE_BRANCH
		to_done = p
	} else {
		// Such a large stack we need to protect against wraparound.
		// If SP is close to zero:
		//	SP-stackguard+StackGuard <= framesize + (StackGuard-StackSmall)
		// The +StackGuard on both sides is required to keep the left side positive:
		// SP is allowed to be slightly below stackguard. See stack.h.
		//
		// Preemption sets stackguard to StackPreempt, a very large value.
		// That breaks the math above, so we have to check for that explicitly.
		//	// stackguard is A0
		//	MOV	$StackPreempt, A1
		//	BEQ	A0, A1, more
		//	ADD	$StackGuard, SP, A1
		//	SUB	A0, A1
		//	MOV	$(framesize+(StackGuard-StackSmall)), A0
		//	BGTU	A1, A0, done
		p = obj.Appendp(p, c.newprog)
		p.As = AMOV
		p.From.Type = obj.TYPE_CONST
		p.From.Offset = objabi.StackPreempt
		p.To.Type = obj.TYPE_REG
		p.To.Reg = REG_A1

		p = obj.Appendp(p, c.newprog)
		to_more = p
		p.As = ABEQ
		p.From.Type = obj.TYPE_REG
		p.From.Reg = REG_A0
		p.Reg = REG_A1
		p.To.Type = obj.TYPE_BRANCH

		p = obj.Appendp(p, c.newprog)
		p.As = AADDI
		p.From.Type = obj.TYPE_CONST
		p.From.Offset = int64(objabi.StackGuard)
		p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_X2})
		p.To.Type = obj.TYPE_REG
		p.To.Reg = REG_A1

		p = obj.Appendp(p, c.newprog)
		p.As = ASUB
		p.From.Type = obj.TYPE_REG
		p.From.Reg = REG_A0
		p.SetFrom3(obj.Addr{Type: obj.TYPE_REG, Reg: REG_A1})
		p.To.Type = obj.TYPE_REG
		p.To.Reg = REG_A1

		p = obj.Appendp(p, c.newprog)
		p.As = AMOV
		p.From.Type = obj.TYPE_CONST
		p.From.Offset = int64(framesize) + int64(objabi.StackGuard) - objabi.StackSmall
		p.To.Type = obj.TYPE_REG
		p.To.Reg = REG_A0

		p = obj.Appendp(p, c.newprog)
		p.As = ABLTU
		p.From.Type = obj.TYPE_REG
		p.From.Reg = REG_A0
		p.Reg = REG_A1
		p.To.Type = obj.TYPE_BRANCH
		to_done = p
	}

	// JAL	runtime.morestack(SB)
	p = obj.Appendp(p, c.newprog)
	p.As = obj.ACALL
	p.Reg = REG_T0
	p.To.Type = obj.TYPE_BRANCH
	if c.cursym.CFunc() {
		p.To.Sym = c.ctxt.Lookup("runtime.morestackc")
	} else if c.cursym.Func.Text.GetFrom3().Offset&obj.NEEDCTXT == 0 {
		p.To.Sym = c.ctxt.Lookup("runtime.morestack_noctxt")
	} else {
		p.To.Sym = c.ctxt.Lookup("runtime.morestack")
	}
	if to_more != nil {
		to_more.Pcond = p
	}
	p = c.jalrToSym(p, REG_T0)

	// JMP	start
	p = obj.Appendp(p, c.newprog)
	p.As = AJAL
	p.To = obj.Addr{Type: obj.TYPE_BRANCH}
	p.From = obj.Addr{Type: obj.TYPE_REG, Reg: REG_ZERO}
	p.Pcond = c.cursym.Func.Text.Link

	// placeholder for to_done's jump target
	p = obj.Appendp(p, c.newprog)
	p.As = obj.ANOP // zero-width place holder
	to_done.Pcond = p

	return p
}

// signExtend sign extends val starting at bit bit.
func signExtend(val int64, bit uint) int64 {
	// Mask off the bits to keep.
	low := val
	low &= 1<<bit - 1

	// Generate upper sign bits, leaving space for the bottom bits.
	val >>= bit - 1
	val <<= 63
	val >>= 64 - bit
	val |= low // put the low bits into place.

	return val
}

// Split32BitImmediate splits a signed 32-bit immediate into a signed 20-bit
// upper immediate and a signed 12-bit lower immediate to be added to the upper
// result.
//
// For example, high may be used in LUI and low in a following ADDI to generate
// a full 32-bit constant.
func Split32BitImmediate(imm int64) (low, high int64, err error) {
	if !immFits(imm, 32) {
		return 0, 0, fmt.Errorf("immediate does not fit in 32-bits: %d", imm)
	}

	// Nothing special needs to be done if the immediate fits in 12-bits.
	if immFits(imm, 12) {
		return imm, 0, nil
	}

	high = imm >> 12
	// The bottom 12 bits will be treated as signed.
	//
	// If that will result in a negative 12 bit number, add 1 to
	// our upper bits to adjust for the borrow.
	//
	// It is not possible for this increment to overflow. To
	// overflow, the 20 top bits would be 1, and the sign bit for
	// the low 12 bits would be set, in which case the entire 32
	// bit pattern fits in a 12 bit signed value.
	if imm&(1<<11) != 0 {
		high++
	}

	high = signExtend(high, 20)
	low = signExtend(imm, 12)

	return
}

func regval(r int16, min int16, max int16) uint32 {
	if r < min || max < r {
		panic(fmt.Sprintf("register out of range, want %d < %d < %d", min, r, max))
	}
	return uint32(r - min)
}

func reg(a obj.Addr, min int16, max int16) uint32 {
	if a.Type != obj.TYPE_REG {
		panic(fmt.Sprintf("ill typed: %+v", a))
	}
	return regval(a.Reg, min, max)
}

// regi extracts the integer register from an Addr.
func regi(a obj.Addr) uint32 { return reg(a, REG_X0, REG_X31) }

// regf extracts the float register from an Addr.
func regf(a obj.Addr) uint32 { return reg(a, REG_F0, REG_F31) }

func wantReg(p *obj.Prog, pos string, a *obj.Addr, descr string, min int16, max int16) {
	if a == nil {
		p.Ctxt.Diag("%v\texpected register in %s position but got nothing",
			p, pos)
		return
	}
	if a.Type != obj.TYPE_REG {
		p.Ctxt.Diag("%v\texpected register in %s position but got %s",
			p, pos, obj.Dconv(nil, a))
		return
	}
	if a.Reg < min || max < a.Reg {
		p.Ctxt.Diag("%v\texpected %s register in %s position but got non-%s register %s",
			p, descr, pos, descr, obj.Dconv(nil, a))
	}
}

// wantIntReg checks that a contains an integer register.
func wantIntReg(p *obj.Prog, pos string, a *obj.Addr) {
	wantReg(p, pos, a, "integer", REG_X0, REG_X31)
}

// wantFloatReg checks that a contains a floating-point register.
func wantFloatReg(p *obj.Prog, pos string, a *obj.Addr) {
	wantReg(p, pos, a, "float", REG_F0, REG_F31)
}

// immFits reports whether immediate value x fits in nbits bits.
func immFits(x int64, nbits uint) bool {
	nbits--
	var min int64 = -1 << nbits
	var max int64 = 1<<nbits - 1
	return min <= x && x <= max
}

// immi extracts the integer literal of the specified size from an Addr.
func immi(a obj.Addr, nbits uint) uint32 {
	if a.Type != obj.TYPE_CONST {
		panic(fmt.Sprintf("ill typed: %+v", a))
	}
	if !immFits(a.Offset, nbits) {
		panic(fmt.Sprintf("immediate %d in %v cannot fit in %d bits", a.Offset, a, nbits))
	}
	return uint32(a.Offset)
}

func wantImm(p *obj.Prog, pos string, a obj.Addr, nbits uint) {
	if a.Type != obj.TYPE_CONST {
		p.Ctxt.Diag("%v\texpected immediate in %s position but got %s", p, pos, obj.Dconv(nil, &a))
		return
	}
	if !immFits(a.Offset, nbits) {
		p.Ctxt.Diag("%v\timmediate in %s position cannot be larger than %d bits but got %d", p, pos, nbits, a.Offset)
	}
}

func wantEvenJumpOffset(p *obj.Prog) {
	if p.To.Offset%1 != 0 {
		p.Ctxt.Diag("%v\tjump offset %v must be even", p, obj.Dconv(nil, &p.To))
	}
}

func validateRIII(p *obj.Prog) {
	wantIntReg(p, "from", &p.From)
	wantIntReg(p, "from3", p.GetFrom3())
	wantIntReg(p, "to", &p.To)
}

func validateRFFF(p *obj.Prog) {
	wantFloatReg(p, "from", &p.From)
	wantFloatReg(p, "from3", p.GetFrom3())
	wantFloatReg(p, "to", &p.To)
}

func validateRFFI(p *obj.Prog) {
	wantFloatReg(p, "from", &p.From)
	wantFloatReg(p, "from3", p.GetFrom3())
	wantIntReg(p, "to", &p.To)
}

func validateRFI(p *obj.Prog) {
	wantFloatReg(p, "from", &p.From)
	wantIntReg(p, "to", &p.To)
}

func validateRIF(p *obj.Prog) {
	wantIntReg(p, "from", &p.From)
	wantFloatReg(p, "to", &p.To)
}

func validateRFF(p *obj.Prog) {
	wantFloatReg(p, "from", &p.From)
	wantFloatReg(p, "to", &p.To)
}

func encodeR(p *obj.Prog, rs1 uint32, rs2 uint32, rd uint32) uint32 {
	i, ok := encode(p.As)
	if !ok {
		panic("encodeR: could not encode instruction")
	}
	if i.rs2 != 0 && rs2 != 0 {
		panic("encodeR: instruction uses rs2, but rs2 was nonzero")
	}

	// Using Scond for the floating-point rounding mode override
	// TODO(sorear) is there a more appropriate way to handle opcode extension bits like this?
	return i.funct7<<25 | i.rs2<<20 | rs2<<20 | rs1<<15 | i.funct3<<12 | uint32(p.Scond)<<12 | rd<<7 | i.opcode
}

func encodeRIII(p *obj.Prog) uint32 {
	return encodeR(p, regi(*p.GetFrom3()), regi(p.From), regi(p.To))
}

func encodeRFFF(p *obj.Prog) uint32 {
	return encodeR(p, regf(*p.GetFrom3()), regf(p.From), regf(p.To))
}

func encodeRFFI(p *obj.Prog) uint32 {
	return encodeR(p, regf(*p.GetFrom3()), regf(p.From), regi(p.To))
}

func encodeRFI(p *obj.Prog) uint32 {
	return encodeR(p, regf(p.From), 0, regi(p.To))
}

func encodeRIF(p *obj.Prog) uint32 {
	return encodeR(p, regi(p.From), 0, regf(p.To))
}

func encodeRFF(p *obj.Prog) uint32 {
	return encodeR(p, regf(p.From), 0, regf(p.To))
}

func validateII(p *obj.Prog) {
	wantImm(p, "from", p.From, 12)
	wantIntReg(p, "from3", p.GetFrom3())
	wantIntReg(p, "to", &p.To)
}

func validateIF(p *obj.Prog) {
	wantImm(p, "from", p.From, 12)
	wantIntReg(p, "from3", p.GetFrom3())
	wantFloatReg(p, "to", &p.To)
}

func encodeI(p *obj.Prog, rd uint32) uint32 {
	imm := immi(p.From, 12)
	rs1 := regi(*p.GetFrom3())
	i, ok := encode(p.As)
	if !ok {
		panic("encodeI: could not encode instruction")
	}
	imm |= uint32(i.csr)
	return imm<<20 | rs1<<15 | i.funct3<<12 | rd<<7 | i.opcode
}

func encodeII(p *obj.Prog) uint32 {
	return encodeI(p, regi(p.To))
}

func encodeIF(p *obj.Prog) uint32 {
	return encodeI(p, regf(p.To))
}

func validateSI(p *obj.Prog) {
	wantImm(p, "from", p.From, 12)
	wantIntReg(p, "from3", p.GetFrom3())
	wantIntReg(p, "to", &p.To)
}

func validateSF(p *obj.Prog) {
	wantImm(p, "from", p.From, 12)
	wantFloatReg(p, "from3", p.GetFrom3())
	wantIntReg(p, "to", &p.To)
}

func EncodeSImmediate(imm int64) (int64, error) {
	if !immFits(imm, 12) {
		return 0, fmt.Errorf("immediate %#x does not fit in 12 bits", imm)
	}

	return ((imm >> 5) << 25) | ((imm & 0x1f) << 7), nil
}

func encodeS(p *obj.Prog, rs2 uint32) uint32 {
	imm := immi(p.From, 12)
	rs1 := regi(p.To)
	i, ok := encode(p.As)
	if !ok {
		panic("encodeS: could not encode instruction")
	}
	return (imm>>5)<<25 |
		rs2<<20 |
		rs1<<15 |
		i.funct3<<12 |
		(imm&0x1f)<<7 |
		i.opcode
}

func encodeSI(p *obj.Prog) uint32 {
	return encodeS(p, regi(*p.GetFrom3()))
}

func encodeSF(p *obj.Prog) uint32 {
	return encodeS(p, regf(*p.GetFrom3()))
}

func validateSB(p *obj.Prog) {
	// Offsets are multiples of two, so accept 13 bit immediates for the 12 bit slot.
	// We implicitly drop the least significant bit in encodeSB.
	wantEvenJumpOffset(p)
	wantImm(p, "to", p.To, 13)
	// TODO: validate that the register from p.Reg is in range
	wantIntReg(p, "from", &p.From)
}

func encodeSB(p *obj.Prog) uint32 {
	imm := immi(p.To, 13)
	rs2 := regval(p.Reg, REG_X0, REG_X31)
	rs1 := regi(p.From)
	i, ok := encode(p.As)
	if !ok {
		panic("encodeSB: could not encode instruction")
	}
	return (imm>>12)<<31 |
		((imm>>5)&0x3f)<<25 |
		rs2<<20 |
		rs1<<15 |
		i.funct3<<12 |
		((imm>>1)&0xf)<<8 |
		((imm>>11)&0x1)<<7 |
		i.opcode
}

func validateU(p *obj.Prog) {
	if p.As == AAUIPC && p.Mark&(NEED_PCREL_ITYPE_RELOC|NEED_PCREL_STYPE_RELOC) != 0 {
		// TODO(sorear): Hack.  The Offset is being used here to temporarily
		// store the relocation addend, not as an actual offset to assemble,
		// so it's OK for it to be out of range.  Is there a more valid way
		// to represent this state?
		return
	}
	wantImm(p, "from", p.From, 20)
	wantIntReg(p, "to", &p.To)
}

func encodeU(p *obj.Prog) uint32 {
	// The immediates for encodeU are the upper 20 bits of a 32 bit value.
	// Rather than have the user/compiler generate a 32 bit constant,
	// the bottommost bits of which must all be zero,
	// instead accept just the top bits.
	imm := immi(p.From, 20)
	rd := regi(p.To)
	i, ok := encode(p.As)
	if !ok {
		panic("encodeU: could not encode instruction")
	}
	return imm<<12 | rd<<7 | i.opcode
}

func EncodeIImmediate(imm int64) (int64, error) {
	if !immFits(imm, 12) {
		return 0, fmt.Errorf("immediate %#x does not fit in 12 bits", imm)
	}

	return imm << 20, nil
}

func EncodeUImmediate(imm int64) (int64, error) {
	if !immFits(imm, 20) {
		return 0, fmt.Errorf("immediate %#x does not fit in 20 bits", imm)
	}

	return imm << 12, nil
}

func validateUJ(p *obj.Prog) {
	// Offsets are multiples of two, so accept 21 bit immediates for the 20 bit slot.
	// We implicitly drop the least significant bit in encodeUJ.
	wantEvenJumpOffset(p)
	wantImm(p, "to", p.To, 21)
	wantIntReg(p, "from", &p.From)
}

// encodeUJImmediate encodes a UJ-type immediate. imm must fit in 21-bits.
func encodeUJImmediate(imm uint32) uint32 {
	return (imm>>20)<<31 |
		((imm>>1)&0x3ff)<<21 |
		((imm>>11)&0x1)<<20 |
		((imm>>12)&0xff)<<12
}

// EncodeUJImmediate encodes a UJ-type immediate.
func EncodeUJImmediate(imm int64) (uint32, error) {
	if !immFits(imm, 21) {
		return 0, fmt.Errorf("immediate %#x does not fit in 21 bits", imm)
	}
	return encodeUJImmediate(uint32(imm)), nil
}

func encodeUJ(p *obj.Prog) uint32 {
	imm := encodeUJImmediate(immi(p.To, 21))
	rd := regi(p.From)
	i, ok := encode(p.As)
	if !ok {
		panic("encodeUJ: could not encode instruction")
	}
	return imm | rd<<7 | i.opcode
}

func validateRaw(p *obj.Prog) {
	// Treat the raw value specially as a 32-bit unsigned integer. Nobody
	// wants to enter negative machine code.
	a := p.From
	if a.Type != obj.TYPE_CONST {
		p.Ctxt.Diag("%v\texpected immediate in raw position but got %s", p, obj.Dconv(nil, &a))
		return
	}
	if a.Offset < 0 || 1<<32 <= a.Offset {
		p.Ctxt.Diag("%v\timmediate in raw position cannot be larger than 32 bits but got %d", p, a.Offset)
	}
}

func encodeRaw(p *obj.Prog) uint32 {
	// Treat the raw value specially as a 32-bit unsigned integer. Nobody
	// wants to enter negative machine code.
	a := p.From
	if a.Type != obj.TYPE_CONST {
		panic(fmt.Sprintf("ill typed: %+v", a))
	}
	if a.Offset < 0 || 1<<32 <= a.Offset {
		panic(fmt.Sprintf("immediate %d in %v cannot fit in 32 bits", a.Offset, a))
	}
	return uint32(a.Offset)
}

type encoding struct {
	encode   func(*obj.Prog) uint32 // encode returns the machine code for a Prog
	validate func(*obj.Prog)        // validate validates a Prog, calling ctxt.Diag for any issues
	length   int64                  // length of encoded instruction; 0 for pseudo-ops, 4 otherwise
}

var (
	// Encodings have the following naming convention:
	//	1. the instruction encoding (R/I/S/SB/U/UJ), in lowercase
	//	2. zero or more register operand identifiers (I = integer
	//	   register, F = float register), in uppercase
	//	3. the word "Encoding"
	// For example, rIIIEncoding indicates an R-type instruction with two
	// integer register inputs and an integer register output; sFEncoding
	// indicates an S-type instruction with rs2 being a float register.

	rIIIEncoding = encoding{encode: encodeRIII, validate: validateRIII, length: 4}
	rFFFEncoding = encoding{encode: encodeRFFF, validate: validateRFFF, length: 4}
	rFFIEncoding = encoding{encode: encodeRFFI, validate: validateRFFI, length: 4}
	rFIEncoding  = encoding{encode: encodeRFI, validate: validateRFI, length: 4}
	rIFEncoding  = encoding{encode: encodeRIF, validate: validateRIF, length: 4}
	rFFEncoding  = encoding{encode: encodeRFF, validate: validateRFF, length: 4}

	iIEncoding = encoding{encode: encodeII, validate: validateII, length: 4}
	iFEncoding = encoding{encode: encodeIF, validate: validateIF, length: 4}

	sIEncoding = encoding{encode: encodeSI, validate: validateSI, length: 4}
	sFEncoding = encoding{encode: encodeSF, validate: validateSF, length: 4}

	sbEncoding = encoding{encode: encodeSB, validate: validateSB, length: 4}

	uEncoding = encoding{encode: encodeU, validate: validateU, length: 4}

	ujEncoding = encoding{encode: encodeUJ, validate: validateUJ, length: 4}

	rawEncoding = encoding{encode: encodeRaw, validate: validateRaw, length: 4}

	// pseudoOpEncoding panics if encoding is attempted, but does no validation.
	pseudoOpEncoding = encoding{encode: nil, validate: func(*obj.Prog) {}, length: 0}

	// badEncoding is used when an invalid op is encountered.
	// An error has already been generated, so let anything else through.
	badEncoding = encoding{encode: func(*obj.Prog) uint32 { return 0 }, validate: func(*obj.Prog) {}, length: 0}
)

// encodingForAs contains the encoding for a RISC-V instruction.
// Instructions are masked with obj.AMask to keep indices small.
// TODO: merge this with the encoding table in inst.go.
// TODO: add other useful per-As info, like whether it is a branch (used in preprocess).
var encodingForAs = [...]encoding{
	// 2.5: Control Transfer Instructions
	AJAL & obj.AMask:  ujEncoding,
	AJALR & obj.AMask: iIEncoding,
	ABEQ & obj.AMask:  sbEncoding,
	ABNE & obj.AMask:  sbEncoding,
	ABLT & obj.AMask:  sbEncoding,
	ABLTU & obj.AMask: sbEncoding,
	ABGE & obj.AMask:  sbEncoding,
	ABGEU & obj.AMask: sbEncoding,

	// 2.9: Environment Call and Breakpoints
	AECALL & obj.AMask:  iIEncoding,
	AEBREAK & obj.AMask: iIEncoding,

	// 4.2: Integer Computational Instructions
	AADDI & obj.AMask:  iIEncoding,
	AADDIW & obj.AMask: iIEncoding,
	ASLTI & obj.AMask:  iIEncoding,
	ASLTIU & obj.AMask: iIEncoding,
	AANDI & obj.AMask:  iIEncoding,
	AORI & obj.AMask:   iIEncoding,
	AXORI & obj.AMask:  iIEncoding,
	ASLLI & obj.AMask:  iIEncoding,
	ASRLI & obj.AMask:  iIEncoding,
	ASRAI & obj.AMask:  iIEncoding,
	ALUI & obj.AMask:   uEncoding,
	AAUIPC & obj.AMask: uEncoding,
	AADD & obj.AMask:   rIIIEncoding,
	ASLT & obj.AMask:   rIIIEncoding,
	ASLTU & obj.AMask:  rIIIEncoding,
	AAND & obj.AMask:   rIIIEncoding,
	AOR & obj.AMask:    rIIIEncoding,
	AXOR & obj.AMask:   rIIIEncoding,
	ASLL & obj.AMask:   rIIIEncoding,
	ASRL & obj.AMask:   rIIIEncoding,
	ASUB & obj.AMask:   rIIIEncoding,
	ASRA & obj.AMask:   rIIIEncoding,

	// 4.3: Load and Store Instructions
	ALD & obj.AMask:  iIEncoding,
	ALW & obj.AMask:  iIEncoding,
	ALWU & obj.AMask: iIEncoding,
	ALH & obj.AMask:  iIEncoding,
	ALHU & obj.AMask: iIEncoding,
	ALB & obj.AMask:  iIEncoding,
	ALBU & obj.AMask: iIEncoding,
	ASD & obj.AMask:  sIEncoding,
	ASW & obj.AMask:  sIEncoding,
	ASH & obj.AMask:  sIEncoding,
	ASB & obj.AMask:  sIEncoding,

	// 4.4: System Instructions
	ARDCYCLE & obj.AMask:   iIEncoding,
	ARDTIME & obj.AMask:    iIEncoding,
	ARDINSTRET & obj.AMask: iIEncoding,

	// 5.1: Multiplication Operations
	AMUL & obj.AMask:    rIIIEncoding,
	AMULH & obj.AMask:   rIIIEncoding,
	AMULHU & obj.AMask:  rIIIEncoding,
	AMULHSU & obj.AMask: rIIIEncoding,
	AMULW & obj.AMask:   rIIIEncoding,
	ADIV & obj.AMask:    rIIIEncoding,
	ADIVU & obj.AMask:   rIIIEncoding,
	AREM & obj.AMask:    rIIIEncoding,
	AREMU & obj.AMask:   rIIIEncoding,
	ADIVW & obj.AMask:   rIIIEncoding,
	ADIVUW & obj.AMask:  rIIIEncoding,
	AREMW & obj.AMask:   rIIIEncoding,
	AREMUW & obj.AMask:  rIIIEncoding,

	// 7.5: Single-Precision Load and Store Instructions
	AFLW & obj.AMask: iFEncoding,
	AFSW & obj.AMask: sFEncoding,

	// 7.6: Single-Precision Floating-Point Computational Instructions
	AFADDS & obj.AMask:  rFFFEncoding,
	AFSUBS & obj.AMask:  rFFFEncoding,
	AFMULS & obj.AMask:  rFFFEncoding,
	AFDIVS & obj.AMask:  rFFFEncoding,
	AFSQRTS & obj.AMask: rFFFEncoding,

	// 7.7: Single-Precision Floating-Point Conversion and Move Instructions
	AFCVTWS & obj.AMask:  rFIEncoding,
	AFCVTLS & obj.AMask:  rFIEncoding,
	AFCVTSW & obj.AMask:  rIFEncoding,
	AFCVTSL & obj.AMask:  rIFEncoding,
	AFSGNJS & obj.AMask:  rFFFEncoding,
	AFSGNJNS & obj.AMask: rFFFEncoding,
	AFSGNJXS & obj.AMask: rFFFEncoding,
	AFMVSX & obj.AMask:   rIFEncoding,

	// 7.8: Single-Precision Floating-Point Compare Instructions
	AFEQS & obj.AMask: rFFIEncoding,
	AFLTS & obj.AMask: rFFIEncoding,
	AFLES & obj.AMask: rFFIEncoding,

	// 8.2: Double-Precision Load and Store Instructions
	AFLD & obj.AMask: iFEncoding,
	AFSD & obj.AMask: sFEncoding,

	// 8.3: Double-Precision Floating-Point Computational Instructions
	AFADDD & obj.AMask:  rFFFEncoding,
	AFSUBD & obj.AMask:  rFFFEncoding,
	AFMULD & obj.AMask:  rFFFEncoding,
	AFDIVD & obj.AMask:  rFFFEncoding,
	AFSQRTD & obj.AMask: rFFFEncoding,

	// 8.4: Double-Precision Floating-Point Conversion and Move Instructions
	AFCVTWD & obj.AMask:  rFIEncoding,
	AFCVTLD & obj.AMask:  rFIEncoding,
	AFCVTDW & obj.AMask:  rIFEncoding,
	AFCVTDL & obj.AMask:  rIFEncoding,
	AFCVTSD & obj.AMask:  rFFEncoding,
	AFCVTDS & obj.AMask:  rFFEncoding,
	AFSGNJD & obj.AMask:  rFFFEncoding,
	AFSGNJND & obj.AMask: rFFFEncoding,
	AFSGNJXD & obj.AMask: rFFFEncoding,
	AFMVDX & obj.AMask:   rIFEncoding,

	// 8.5: Double-Precision Floating-Point Compare Instructions
	AFEQD & obj.AMask: rFFIEncoding,
	AFLTD & obj.AMask: rFFIEncoding,
	AFLED & obj.AMask: rFFIEncoding,

	// Escape hatch
	AWORD & obj.AMask: rawEncoding,

	// Pseudo-operations
	obj.AFUNCDATA: pseudoOpEncoding,
	obj.APCDATA:   pseudoOpEncoding,
	obj.ATEXT:     pseudoOpEncoding,
	obj.ANOP:      pseudoOpEncoding,
}

// encodingForP returns the encoding (encode+validate funcs) for a Prog.
func encodingForP(p *obj.Prog) encoding {
	if base := p.As &^ obj.AMask; base != obj.ABaseRISCV64 && base != 0 {
		p.Ctxt.Diag("encodingForP: not a RISC-V instruction %s", p.As)
		return badEncoding
	}
	as := p.As & obj.AMask
	if int(as) >= len(encodingForAs) {
		p.Ctxt.Diag("encodingForP: bad RISC-V instruction %s", p.As)
		return badEncoding
	}
	enc := encodingForAs[as]
	if enc.validate == nil {
		p.Ctxt.Diag("encodingForP: no encoding for instruction %s", p.As)
		return badEncoding
	}
	return enc
}

// assemble emits machine code.
// It is called at the very end of the assembly process.
func assemble(ctxt *obj.Link, cursym *obj.LSym, newprog obj.ProgAlloc) {
	p := cursym.Func.Text
	if p == nil || p.Link == nil { // handle external functions and ELF section symbols
		return
	}

	c := ctxtRiscv{ctxt: ctxt, newprog: newprog, cursym: cursym, autosize: int32(p.To.Offset)}

	var symcode []uint32 // machine code for this symbol
	for p := c.cursym.Func.Text; p != nil; p = p.Link {
		switch p.As {
		case AJALR:
			if p.To.Sym != nil {
				// This is a CALL/JMP. We add a relocation only
				// for linker stack checking. No actual
				// relocation is needed.
				rel := obj.Addrel(cursym)
				rel.Off = int32(p.Pc)
				rel.Siz = 4
				rel.Sym = p.To.Sym
				rel.Add = p.To.Offset
				rel.Type = objabi.R_CALLRISCV
			}
		case AAUIPC:
			var t objabi.RelocType
			if p.Mark&NEED_PCREL_ITYPE_RELOC == NEED_PCREL_ITYPE_RELOC {
				t = objabi.R_RISCV_PCREL_ITYPE
			} else if p.Mark&NEED_PCREL_STYPE_RELOC == NEED_PCREL_STYPE_RELOC {
				t = objabi.R_RISCV_PCREL_STYPE
			} else {
				break
			}
			if p.Link == nil {
				ctxt.Diag("AUIPC needing PC-relative reloc missing following instruction")
				break
			}
			if p.From.Sym == nil {
				ctxt.Diag("AUIPC needing PC-relative reloc missing symbol")
				break
			}

			rel := obj.Addrel(cursym)
			rel.Off = int32(p.Pc)
			rel.Siz = 8
			rel.Sym = p.From.Sym
			rel.Add = p.From.Offset
			p.From.Offset = 0 // relocation offset can be larger than the maximum size of an auipc, so don't accidentally assemble it
			rel.Type = t
		}

		enc := encodingForP(p)
		if enc.length > 0 {
			symcode = append(symcode, enc.encode(p))
		}
	}
	cursym.Size = int64(4 * len(symcode))

	cursym.Grow(cursym.Size)
	for p, i := cursym.P, 0; i < len(symcode); p, i = p[4:], i+1 {
		ctxt.Arch.ByteOrder.PutUint32(p, symcode[i])
	}
}
