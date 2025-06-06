package fun

import (
	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/cache"
	"github.com/iigor000/database/structures/memtable"
)

type Database struct {
	//wal
	//compression
	memtables memtable.Memtables
	config    config.BlockConfig
	cache     cache.Cache
}

func NewDatabase(config config.BlockConfig) (*Database, error) {
}
