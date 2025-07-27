package block_organization

import (
	"fmt"
	"io"
	"os"

	"github.com/iigor000/database/config"
)

// BlockManager je struktura koja omogucava citanje i pisanje blokova podataka na disku
type BlockManager struct {
	BlockSize int
}

func NewBlockManager(cfg *config.Config) *BlockManager {
	return &BlockManager{
		BlockSize: cfg.Block.BlockSize,
	}
}

// Funkcija koja cita blok podataka sa diska
func (bm *BlockManager) ReadBlock(filePath string, blockNumber int) ([]byte, error) {

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

	// Proveravamo da li je duzina podataka veca od BlockSize
	if len(data) > bm.BlockSize {
		return fmt.Errorf("data size exceeds block size: %d bits > %d bits", len(data), bm.BlockSize)
	}
	// Ako jeste, onda bacamo gresku jer ne mozemo da upisemo vise podataka nego sto je dozvoljeno
	if len(data) < bm.BlockSize {
		// Ako je duzina manja, onda popunjavamo ostatak bloka nulama
		data = append(data, make([]byte, bm.BlockSize-len(data))...)
	}

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

// Funkcija koja dodaje blok podataka na kraj fajla i vraca broj bloka
func (bm *BlockManager) AppendBlock(filePath string, data []byte) (int, error) {

	// Proveravamo da li je duzina podataka veca od BlockSize
	if len(data) > bm.BlockSize {
		return 0, fmt.Errorf("data size exceeds block size: %d > %d", len(data), bm.BlockSize)
	}
	// Ako jeste, onda bacamo gresku jer ne mozemo da upisemo vise podataka nego sto je dozvoljeno
	if len(data) < bm.BlockSize {
		// Ako je duzina manja, onda popunjavamo ostatak bloka nulama
		data = append(data, make([]byte, bm.BlockSize-len(data))...)
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	offset, err := file.Seek(0, io.SeekEnd) // Pozicioniramo se na kraj fajla
	if err != nil {
		return 0, err
	}
	_, err = file.Write(data) // Upisujemo podatke na kraj fajla
	if err != nil {
		return 0, err
	}
	blockNumber := int(offset / int64(bm.BlockSize)) // Izracunavamo broj bloka na osnovu offseta
	return blockNumber, nil                          // Vracamo broj bloka i potencijalnu gresku
}

func (bm *BlockManager) Write(filePath string, blockNumber int, data []byte) error {
	// Proveravamo da li je duzina podataka veca od BlockSize, ako jeste deilmo ga na blokove
	// Prvi bajt svakog bloka je oznaka da li je block krajnji, srednji ili prvi
	if len(data) > bm.BlockSize+1 {
		blocks := make([][]byte, 0)
		for i := 0; i < len(data); i += bm.BlockSize - 1 {
			end := i + bm.BlockSize - 1
			if end > len(data) {
				end = len(data)
			}
			block := make([]byte, bm.BlockSize)
			copy(block[1:], data[i:end]) // Kopiramo podatke u blok,
			if i == 0 {
				block[0] = 1 // Prvi blok
			} else if end >= len(data) {
				block[0] = 2 // Poslednji blok
			} else {
				block[0] = 3 // Srednji blok
			}
			blocks = append(blocks, block)
		}

		for i, block := range blocks {
			if err := bm.WriteBlock(filePath, blockNumber+i, block); err != nil {
				return fmt.Errorf("error writing block %d: %w", blockNumber+i, err)
			}
		}
		return nil
	}

	block := make([]byte, bm.BlockSize)
	copy(block[1:], data) // Kopiramo podatke u blok,
	block[0] = 2          // Poslednji blok

	return bm.WriteBlock(filePath, blockNumber, block) // Pisemo blok na disk
}

func (bm *BlockManager) Append(filePath string, data []byte) (int, error) {
	// Proveravamo da li je duzina podataka veca od BlockSize, ako jeste deilmo ga na blokove
	// Prvi bajt svakog bloka je oznaka da li je block krajnji, srednji ili prvi
	if len(data) > bm.BlockSize+1 {
		blocks := make([][]byte, 0)
		for i := 0; i < len(data); i += bm.BlockSize - 1 {
			end := i + bm.BlockSize - 1
			if end > len(data) {
				end = len(data)
			}
			block := make([]byte, bm.BlockSize)
			copy(block[1:], data[i:end]) // Kopiramo podatke u blok,
			if i == 0 {
				block[0] = 1 // Prvi blok
			} else if end >= len(data) {
				block[0] = 2 // Poslednji blok
			} else {
				block[0] = 3 // Srednji blok
			}
			blocks = append(blocks, block)
		}

		firstBlockNumber := 0
		for i, block := range blocks {
			if i == 0 {
				var err error
				firstBlockNumber, err = bm.AppendBlock(filePath, block)
				if err != nil {
					return 0, fmt.Errorf("error writing first block: %w", err)
				}
			} else {
				if _, err := bm.AppendBlock(filePath, block); err != nil {
					return 0, fmt.Errorf("error writing block %d: %w", i, err)
				}
			}
		}
		return firstBlockNumber, nil
	}

	block := make([]byte, bm.BlockSize)
	copy(block[1:], data) // Kopiramo podatke u blok,
	block[0] = 2          // Poslednji blok

	return bm.AppendBlock(filePath, block)
}

func (bm *BlockManager) Read(filePath string, blockNumber int) ([]byte, error) {
	// Citamo blok podataka sa diska
	data := make([]byte, 0)
	for {
		block, err := bm.ReadBlock(filePath, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("error reading block %d: %w", blockNumber, err)
		}

		if block[0] == 2 { // Ako je poslednji blok, vracamo ga
			data = append(data, block[1:]...) // Dodajemo podatke iz bloka u data
			return data, nil
		} else if block[0] == 3 || block[0] == 1 { // Ako je srednji blok, nastavljamo da citamo dalje
			blockNumber++
			data = append(data, block[1:]...) // Dodajemo podatke iz bloka u data
			continue
		}
	}
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

// Funkcija koja omogucava optimizovano dodavanje blokova podataka uz pomoc kesiranja cime se ubrzava sam proces
func (cbm *CachedBlockManager) AppendBlock(filePath string, data []byte) (int, error) {
	blockNumber, err := cbm.BM.AppendBlock(filePath, data)
	if err != nil {
		return 0, err
	}

	cacheKey := fmt.Sprintf("%s:%d", filePath, blockNumber)
	cbm.C.Put(cacheKey, data) // Stavljamo podatke u kes
	return blockNumber, nil
}

// Funkcija koja omogucava optimizovano citanje blokova podataka uz pomoc kesiranja cime se ubrzava sam proces
func (cbm *CachedBlockManager) Read(filePath string, blockNumber int) ([]byte, error) {
	cacheKey := fmt.Sprintf("%s:%d", filePath, blockNumber)
	if data, isThere := cbm.C.Get(cacheKey); isThere { // Ako blok postoji u kesu, vracamo ga
		return data, nil
	}

	// Ako pak bloka nema u kesu, onda ga citamo sa diska uz pomoc BlockManagera i stavljamo u kes
	data, err := cbm.BM.Read(filePath, blockNumber)
	if err != nil {
		return nil, err
	}

	cbm.C.Put(cacheKey, data)
	return data, nil
}

// Funkcija koja omogucava optimizovano pisanje blokova podataka uz pomoc kesiranja cime se ubrzava sam proces
func (cbm *CachedBlockManager) Write(filePath string, blockNumber int, data []byte) error {
	cacheKey := fmt.Sprintf("%s:%d", filePath, blockNumber)
	cbm.C.Put(cacheKey, data) // Stavljamo podatke u kes
	return cbm.BM.Write(filePath, blockNumber, data)
}

// Funkcija koja omogucava optimizovano dodavanje blokova podataka uz pomoc kesiranja cime se ubrzava sam proces
func (cbm *CachedBlockManager) Append(filePath string, data []byte) (int, error) {
	blockNumber, err := cbm.BM.Append(filePath, data)
	if err != nil {
		return 0, err
	}

	cacheKey := fmt.Sprintf("%s:%d", filePath, blockNumber)
	cbm.C.Put(cacheKey, data) // Stavljamo podatke u kes
	return blockNumber, nil
}
