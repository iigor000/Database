package util

func CheckKeyReserved(key string) bool {
	reservedKeysPrefix := []string{
		TokenBucketPrefix,
		BloomFilterPrefix,
		CMSPrefix,
		HLLPrefix,
		SimHashPrefix,
	}

	for _, prefix := range reservedKeysPrefix {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}
