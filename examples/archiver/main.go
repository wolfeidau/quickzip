package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/klauspost/compress/zstd"
	"github.com/wolfeidau/quickzip"
)

var (
	archiveDir = flag.String("archivedir", runtime.GOROOT(), "The directory to use for archive benchmarks")
	bufferSize = flag.Int("buffersize", 2*1024*1024, "The buffer size to use for archiving")
)

func main() {
	flag.Parse()

	filename := flag.Arg(0)
	if filename == "" {
		myUsage()
		os.Exit(1)
	}

	fmt.Printf("Archiving %s to %s\n", filename, *archiveDir)

	f, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Error creating archive: %s\n", err)
		os.Exit(1)
	}
	defer f.Close()

	log.Println("create archive")

	arc, err := quickzip.NewArchiver(f, *archiveDir,
		quickzip.WithArchiverMethod(zstd.ZipMethodWinZip),
		quickzip.WithArchiverBufferSize(*bufferSize),
		quickzip.WithPathPrefix(filepath.Base(*archiveDir)),
	)
	if err != nil {
		fmt.Printf("Error creating archive: %s\n", err)
		os.Exit(1)
	}

	defer arc.Close()

	log.Println("walk archive files")

	files := make(map[string]os.FileInfo)
	size := int64(0)
	err = filepath.Walk(*archiveDir, func(filename string, fi os.FileInfo, err error) error {
		files[filename] = fi
		size += fi.Size()
		log.Printf("%s %d %04o\n", filename, fi.Size(), fi.Mode())
		return nil
	})
	if err != nil {
		fmt.Printf("Error walking archive directory: %s\n", err)
		os.Exit(1)
	}

	log.Println("archive files")

	err = arc.Archive(context.Background(), files)
	if err != nil {
		fmt.Printf("Error creating archive: %s\n", err)
		os.Exit(1)
	}

	written, count := arc.Written()

	log.Printf("Archive %s created count %d compression %f\n", filename, count, deflatePercentage(size, written))
}

func myUsage() {
	fmt.Printf("Usage: %s [OPTIONS] outputfile.zip ...\n", os.Args[0])
	flag.PrintDefaults()
}

// calculte the percentage of the archive that is deflated
func deflatePercentage(total, compressed int64) float64 {
	return float64(compressed) / float64(total) * 100
}
