package block_organization

import (
	"fmt"
	"os"
	"sync"

	"github.com/iigor000/database/config"
)

// BlockManager je struktura koja omogucava citanje i pisanje blokova podataka na disku
type BlockManager struct {
	BlockSize int
	mu        sync.Mutex
}

func NewBlockManager(cfg *config.BlockConfig) *BlockManager {
	return &BlockManager{
		BlockSize: cfg.BlockSize,
	}
}

// Funkcija koja cita blok podataka sa diska
func (bm *BlockManager) ReadBlock(filePath string, blockNumber int) ([]byte, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := blockNumber * bm.BlockSize // Offset je pozicija na disku gde blok pocinje
	_, err = file.Seek(int64(offset), 0) // Seek nam omogucava da se pozicioniramo na njegov pocetak
	if err != nil {
		return nil, err
	}

	block := make([]byte, bm.BlockSize) // Citamo BlockSize bajtova sa diska u blok i vracamo ga i potencijalnu gresku
	_, err = file.Read(block)
	return block, err
}

// Funkcija koja pise blok podataka na disk
func (bm *BlockManager) WriteBlock(filePath string, blockNumber int, data []byte) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Po principu kao kod citanja, izracunamo gde je pocetak bloka i upisemo podatke na tu poziciju
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := blockNumber * bm.BlockSize
	_, err = file.WriteAt(data, int64(offset))
	return err
}

// INTEGRACIJA BLOCK MANAGERA I BLOCK CACHEA

// CachedBlockManager je struktura koja omogucava citanje i pisanje blokova podataka sa kesiranjem
type CachedBlockManager struct {
	BM *BlockManager
	C  *BlockCache
}

// Funkcija koja omogucava optimizovano citanje blokova podataka uz pomoc kesiranja cime se ubrzava sam proces
func (cbm *CachedBlockManager) ReadBlock(filePath string, blockNumber int) ([]byte, error) {
	cacheKey := fmt.Sprintf("%s:%d", filePath, blockNumber)
	if block, isThere := cbm.C.Get(cacheKey); isThere { // Ako blok postoji u kesu, vracamo ga
		return block, nil
	}

	// Ako pak bloka nema u kesu, onda ga citamo sa diska uz pomoc BlockManagera i stavljamo u kes
	block, err := cbm.BM.ReadBlock(filePath, blockNumber)
	if err != nil {
		return nil, err
	}

	cbm.C.Put(cacheKey, block)
	return block, nil
}

// Funkcija koja omogucava optimizovano pisanje blokova podataka uz pomoc kesiranja cime se ubrzava sam proces
func (cbm *CachedBlockManager) WriteBlock(filePath string, blockNumber int, data []byte) error {
	cacheKey := fmt.Sprintf("%s:%d", filePath, blockNumber)
	cbm.C.Put(cacheKey, data)
	return cbm.BM.WriteBlock(filePath, blockNumber, data)
}
