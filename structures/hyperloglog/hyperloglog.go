package hyperloglog

import (
	"crypto/md5"
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION        = 4
	HLL_MAX_PRECISION        = 16
	SEED              uint32 = 782
)

func firstKbits(value uint64, k uint8) uint64 {
	return value >> uint64((64 - k))
}

func trailingZeroBits(value uint64) uint8 {
	return uint8(bits.TrailingZeros64(value))
}

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(data)
	hash := fn.Sum(nil)
	hashUint := binary.BigEndian.Uint64(hash)
	return hashUint
	// return binary.BigEndian.Uint64(fn.Sum(nil))
}

func MakeHyperLogLog(p int) HLL {
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		panic("Precision must be between 4 and 16")
	}
	return HLL{
		m:   1 << uint(p),
		p:   uint8(p),
		reg: make([]uint8, 1<<p),
	}
}

func (hll *HLL) Add(value []byte) {
	hash := Hash(value)
	index := firstKbits(hash, hll.p)
	zeroBits := trailingZeroBits(hash << hll.p)
	if zeroBits > hll.reg[index] {
		hll.reg[index] = zeroBits
	}
}

func Serialize(hll HLL) []byte {
	data := make([]byte, 1+len(hll.reg))
	data[0] = hll.p
	for i, val := range hll.reg {
		data[i+1] = val
	}
	return data
}

func Deserialize(data []byte) HLL {
	if len(data) < 1 {
		panic("Invalid data length")
	}
	p := data[0]
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		panic("Precision must be between 4 and 16")
	}
	reg := make([]uint8, 1<<p)
	for i := 0; i < len(reg); i++ {
		reg[i] = data[i+1]
	}
	return HLL{
		m:   1 << uint(p),
		p:   uint8(p),
		reg: reg,
	}
}
