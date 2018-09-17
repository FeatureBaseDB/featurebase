// +build amd64,!appengine,!gccgo
// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package roaring implements roaring bitmaps with support for incremental changes.
// Frame layout
//	|-----------------------------+---+---+---+---|
// 0	| a_data_ptr                  |   |   |   |   |
// 8	| a_len                       |   |   |   |   |
// 16	| a_cap                       |   |   |   |   |
// 24	| b_data_ptr                  |   |   |   |   |
// 32	| b_len                       |   |   |   |   |
// 40	| b_cap                       |   |   |   |   |
// 48	| c_data_ptr                  |   |   |   |   |
// 56	| c_len                       |   |   |   |   |
// 64	| c_cap                       |   |   |   |   |
// 72	| function return value (int) |   |   |   |   |
//
// func asmAnd(a,b,c []int64)int
TEXT ·asmAnd(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin0:
	VMOVDQA (BX), Y0
	VMOVDQA (DX), Y1
	VPAND   Y0, Y1, Y0
	VMOVUPS Y0, (R15)

	POPCNTQ (R15), BP
	ADDQ    BP, SI
	POPCNTQ 8(R15), BP
	ADDQ    BP, SI
	POPCNTQ 16(R15), BP
	ADDQ    BP, SI
	POPCNTQ 24(R15), BP
	ADDQ    BP, SI
	ADDQ    $32, BX
	ADDQ    $32, DX
	ADDQ    $32, R15
	SUBQ    $4, CX
	JNE     loop_begin0
	MOVQ    SI, ·noname+72(FP)
	VZEROUPPER
	RET

// func asmOr(a,b,c []int64)int
TEXT ·asmOr(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin1:
	VMOVDQA (BX), Y0
	VMOVDQA (DX), Y1
	VPOR    Y0, Y1, Y0
	VMOVUPS Y0, (R15)

	POPCNTQ (R15), BP
	ADDQ    BP, SI
	POPCNTQ 8(R15), BP
	ADDQ    BP, SI
	POPCNTQ 16(R15), BP
	ADDQ    BP, SI
	POPCNTQ 24(R15), BP
	ADDQ    BP, SI

	ADDQ $32, BX
	ADDQ $32, DX
	ADDQ $32, R15
	SUBQ $4, CX
	JNE  loop_begin1
	MOVQ SI, ·noname+72(FP)
	VZEROUPPER
	RET

// func asmXor(a,b,c []int64)int
TEXT ·asmXor(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin2:
	VMOVDQA (BX), Y0
	VMOVDQA (DX), Y1
	VPXOR   Y0, Y1, Y0
	VMOVUPS Y0, (R15)

	POPCNTQ (R15), BP
	ADDQ    BP, SI
	POPCNTQ 8(R15), BP
	ADDQ    BP, SI
	POPCNTQ 16(R15), BP
	ADDQ    BP, SI
	POPCNTQ 24(R15), BP
	ADDQ    BP, SI

	ADDQ $32, BX
	ADDQ $32, DX
	ADDQ $32, R15
	SUBQ $4, CX
	JNE  loop_begin2
	MOVQ SI, ·noname+72(FP)
	VZEROUPPER
	RET

// func asmAndN(a,b,c []int64)int
TEXT ·asmAndN(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin3:
	VMOVDQA (BX), Y0
	VMOVDQA (DX), Y1
	VPANDN  Y0, Y1, Y0
	VMOVUPS Y0, (R15)

	POPCNTQ (R15), BP
	ADDQ    BP, SI
	POPCNTQ 8(R15), BP
	ADDQ    BP, SI
	POPCNTQ 16(R15), BP
	ADDQ    BP, SI
	POPCNTQ 24(R15), BP
	ADDQ    BP, SI

	ADDQ $32, BX
	ADDQ $32, DX
	ADDQ $32, R15
	SUBQ $4, CX
	JNE  loop_begin3
	MOVQ SI, ·noname+72(FP)
	VZEROUPPER
	RET
