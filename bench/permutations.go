package bench

// PermutationGenerator provides a way to pass integer IDs through a permutation
// map that is pseudorandom but repeatable. This could be done with rand.Perm,
// but that would require storing a [Iterations]int64 array, which we want to avoid
// for large values of Iterations.
// It works by using a Linear Congruence Generator (https://en.wikipedia.org/wiki/Linear_congruential_generator)
// with modulus m = Iterations,
// c = an arbitrary prime,
// a = computed to ensure the full period.
// relevant stackoverflow: http://cs.stackexchange.com/questions/29822/lazily-computing-a-random-permutation-of-the-positive-integers
type PermutationGenerator struct {
	a int64
	c int64
	m int64
}

func NewPermutationGenerator(m int64, seed int64) *PermutationGenerator {
	// figure out 'a' and 'c', return PermutationGenerator
	a := LCGmultiplierFromModulus(m, seed)
	c := int64(22695479)
	return &PermutationGenerator{a, c, m}
}

func (p *PermutationGenerator) Next(n int64) int64 {
	// run one step of the LCG
	return (n*p.a + p.c) % p.m
}

// LCG parameters must satisfy three conditions:
// 1. m and c are relatively prime (satisfied for prime c != m)
// 2. a-1 is divisible by all prime factors of m
// 3. a-1 is divisible by 4 if m is divisible by 4
// Additionally, a seed can be used to select between different permutations
func LCGmultiplierFromModulus(m int64, seed int64) int64 {
	factors := primeFactors(m)
	product := int64(1)
	for p := range factors {
		// satisfy condition 2
		product *= p
	}

	if m%4 == 0 {
		// satisfy condition 3
		product *= 2
	}

	return product*seed + 1
}

func primeFactors(n int64) map[int64]int {
	// Returns map of {integerFactor: count, ...}
	// This is a naive algorithm that will not work well for large prime n.
	factors := make(map[int64]int)
	for i := int64(2); i <= n; i++ {
		div, mod := n/i, n%i
		for mod == 0 {
			factors[i] += 1
			n = div
			div, mod = n/i, n%i
		}
	}
	return factors
}
