// +build amd64,!appengine,!gccgo

// Frame layout
//	|-----------------------------+---+---+---+---|
//0	| a_data_ptr                  |   |   |   |   |
//8	| a_len                       |   |   |   |   |
//16	| a_cap                       |   |   |   |   |
//24	| b_data_ptr                  |   |   |   |   |
//32	| b_len                       |   |   |   |   |
//40	| b_cap                       |   |   |   |   |
//48	| c_data_ptr                  |   |   |   |   |
//56	| c_len                       |   |   |   |   |
//64	| c_cap                       |   |   |   |   |
//72	| function return value (int) |   |   |   |   |
//
// func asmAnd(a,b,c []int64)int
TEXT ·asmAnd(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin0:
	VMOVQ    (BX), X0   // A[i]
	VMOVQ    8(BX), X1  // A[i+1]
	VMOVLHPS X0, X1, X0

	VMOVQ       16(BX), X1     // A[i+2]
	VMOVQ       24(BX), X2     // A[i+3]
	VMOVLHPS    X1, X2, X1
	VINSERTI128 $1, X0, Y1, Y0

	VMOVQ    (DX), X1   // B[i]
	VMOVQ    8(DX), X2  // B[i+1]
	VMOVLHPS X1, X2, X1

	VMOVQ    16(DX), X2 // A[i+2]
	VMOVQ    24(DX), X3 // A[i+3]
	VMOVLHPS X2, X3, X2

	VINSERTI128 $1, X1, Y2, Y1

	VPAND   Y0, Y1, Y0
	VPERMPD $27, Y0, Y0
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
	RET

// func asmOr(a,b,c []int64)int
TEXT ·asmOr(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin1:
	VMOVQ    (BX), X0   // A[i]
	VMOVQ    8(BX), X1  // A[i+1]
	VMOVLHPS X0, X1, X0

	VMOVQ       16(BX), X1     // A[i+2]
	VMOVQ       24(BX), X2     // A[i+3]
	VMOVLHPS    X1, X2, X1
	VINSERTI128 $1, X0, Y1, Y0

	VMOVQ    (DX), X1   // B[i]
	VMOVQ    8(DX), X2  // B[i+1]
	VMOVLHPS X1, X2, X1

	VMOVQ    16(DX), X2 // A[i+2]
	VMOVQ    24(DX), X3 // A[i+3]
	VMOVLHPS X2, X3, X2

	VINSERTI128 $1, X1, Y2, Y1

	VPOR    Y0, Y1, Y0
	VPERMPD $27, Y0, Y0
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
	RET

// func asmXor(a,b,c []int64)int
TEXT ·asmXor(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin2:
	VMOVQ    (BX), X0   // A[i]
	VMOVQ    8(BX), X1  // A[i+1]
	VMOVLHPS X0, X1, X0

	VMOVQ       16(BX), X1     // A[i+2]
	VMOVQ       24(BX), X2     // A[i+3]
	VMOVLHPS    X1, X2, X1
	VINSERTI128 $1, X0, Y1, Y0

	VMOVQ    (DX), X1   // B[i]
	VMOVQ    8(DX), X2  // B[i+1]
	VMOVLHPS X1, X2, X1

	VMOVQ    16(DX), X2 // A[i+2]
	VMOVQ    24(DX), X3 // A[i+3]
	VMOVLHPS X2, X3, X2

	VINSERTI128 $1, X1, Y2, Y1

	VPXOR   Y0, Y1, Y0
	VPERMPD $27, Y0, Y0
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
	RET

// func asmAndN(a,b,c []int64)int
TEXT ·asmAndN(SB), 7, $0
	MOVQ $0, SI
	MOVQ a_data+0(FP), BX   // BX = &a[0]
	MOVL a_len+8(FP), CX    // len(a)
	MOVQ b_data+24(FP), DX  // DX = &b[0]
	MOVQ c_data+48(FP), R15 // DX = &c[0]

loop_begin3:
	VMOVQ    (BX), X0   // A[i]
	VMOVQ    8(BX), X1  // A[i+1]
	VMOVLHPS X0, X1, X0

	VMOVQ       16(BX), X1     // A[i+2]
	VMOVQ       24(BX), X2     // A[i+3]
	VMOVLHPS    X1, X2, X1
	VINSERTI128 $1, X0, Y1, Y0

	VMOVQ    (DX), X1   // B[i]
	VMOVQ    8(DX), X2  // B[i+1]
	VMOVLHPS X1, X2, X1

	VMOVQ    16(DX), X2 // A[i+2]
	VMOVQ    24(DX), X3 // A[i+3]
	VMOVLHPS X2, X3, X2

	VINSERTI128 $1, X1, Y2, Y1

	VPANDN  Y0, Y1, Y0
	VPERMPD $27, Y0, Y0
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
	RET
