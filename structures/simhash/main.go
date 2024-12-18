package main

import (
	"crypto/md5"
	"fmt"
	"strings"
)

func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

func main() {
	fmt.Println(GetHashAsString(SimHash("blab ld!")))
}

func Hash(data []byte) []byte {
	fn := md5.New()
	fn.Write(data)
	hash := fn.Sum(nil)

	binaryHash := make([]byte, len(hash)*8)
	for i, b := range hash {
		for j := 0; j < 8; j++ {
			if (b & (1 << (7 - j))) != 0 {
				binaryHash[i*8+j] = 1
			} else {
				binaryHash[i*8+j] = 0
			}
		}
	}
	return binaryHash
}

func SimHash(data string) []byte {
	m := NumerizeTokens(strings.Split(data, " "))

	hashes := make(map[string][]byte)

	for token, _ := range m {
		hashes[token] = Hash([]byte(token))
	}

	hashInt := make([]int, 128)

	for i := 0; i < len(hashInt); i++ {
		for token, value := range hashes {
			if value[i] == 0 {
				hashInt[i] += m[token] * -1
			} else {
				hashInt[i] += m[token] * 1
			}
		}
	}

	hash := make([]byte, 128)

	for i := 0; i < len(hashInt); i++ {
		if hashInt[i] > 0 {
			hash[i] = 1
		} else {
			hash[i] = 0
		}
	}

	return ByteSliceToBits(hash)
}

func ByteSliceToBits(bytes []byte) []byte {
	bits := make([]byte, len(bytes)/8)

	for i, data := range bytes {
		if data == 0 {
			bits[i/8] = bits[i/8] | (1 << (i % 8))
		}
	}

	return bits
}

func NumerizeTokens(tokens []string) map[string]int {
	m := make(map[string]int)

	for _, token := range tokens {
		m[token] = m[token] + 1
	}

	return m
}
