package rollingHash

import (
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"syscall"
)

type MappedRing struct {
	Ring
	file *os.File
}

func MapRing(path string, regionSize, indexSize int) (*MappedRing, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != 0 && info.Size() != int64(regionSize) {
		return nil, errors.New(fmt.Sprintf(
			"File exists but is the wrong size: %v", info))
	}
	if info.Size() == 0 {
		_, err = file.WriteAt([]byte{0}, int64(regionSize))
		if err != nil {
			return nil, err
		}
	}
	region, err := syscall.Mmap(int(file.Fd()), 0, int(regionSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	ring := &MappedRing{Ring{region: region}, file}
	runtime.SetFinalizer(ring, finalizeMappedRing)
	ring.initialize(region, indexSize)

    // TODO: Size checks. Corruption checks.
	return ring, nil
}

func finalizeMappedRing(ring *MappedRing) {
	log.Printf("Unmapping %v", ring.file)
	if err := syscall.Munmap(ring.region); err != nil {
		log.Fatalf("Failed to unmap %v: %v", *ring, err)
	}
	ring.region = nil
	ring.file.Close()
	ring.file = nil
}
