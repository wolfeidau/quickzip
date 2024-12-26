package quickzip

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zip"
	"github.com/klauspost/compress/zstd"
)

const irregularModes = os.ModeSocket | os.ModeDevice | os.ModeCharDevice | os.ModeNamedPipe

var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 32*1024)
	},
}

var (
	defaultCompressor     = FlateCompressor(-1)
	defaultZstdCompressor = ZstdCompressor(int(zstd.SpeedDefault))
)

// Archiver is an opinionated Zip archiver.
//
// Only regular files, symlinks and directories are supported. Only files that
// are children of the specified chroot directory will be archived.
//
// Access permissions, ownership (unix) and modification times are preserved.
type Archiver struct {
	// This 2 fields are accessed via atomic operations
	// They are at the start of the struct so they are properly 8 byte aligned
	written, entries int64

	zw      *zip.Writer
	options archiverOptions
	m       sync.Mutex

	compressors map[uint16]zip.Compressor
}

// NewArchiver returns a new Archiver.
func NewArchiver(w io.Writer, opts ...ArchiverOption) (*Archiver, error) {
	a := &Archiver{
		compressors: make(map[uint16]zip.Compressor),
	}

	a.options.method = zip.Deflate
	a.options.bufferSize = -1
	for _, o := range opts {
		err := o(&a.options)
		if err != nil {
			return nil, err
		}
	}

	a.zw = zip.NewWriter(w)
	a.zw.SetOffset(a.options.offset)

	// register flate compressor
	a.RegisterCompressor(zip.Deflate, defaultCompressor)
	a.RegisterCompressor(zstd.ZipMethodWinZip, defaultZstdCompressor)

	return a, nil
}

// RegisterCompressor registers custom compressors for a specified method ID.
// The common methods Store and Deflate are built in.
func (a *Archiver) RegisterCompressor(method uint16, comp zip.Compressor) {
	a.zw.RegisterCompressor(method, comp)
	a.compressors[method] = comp
}

// Close closes the underlying ZipWriter.
func (a *Archiver) Close() error {
	return a.zw.Close()
}

// Written returns how many bytes and entries have been written to the archive.
// Written can be called whilst archiving is in progress.
func (a *Archiver) Written() (bytes, entries int64) {
	return atomic.LoadInt64(&a.written), atomic.LoadInt64(&a.entries)
}

// Archive archives all files, symlinks and directories.
func (a *Archiver) Archive(ctx context.Context, chroot string, files map[string]os.FileInfo) (err error) {
	if chroot, err = filepath.Abs(chroot); err != nil {
		return err
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	hdrs := make([]zip.FileHeader, len(names))

	for i, name := range names {
		fi := files[name]
		if fi.Mode()&irregularModes != 0 {
			continue
		}

		path, err := filepath.Abs(name)
		if err != nil {
			return err
		}

		if !strings.HasPrefix(path, chroot+string(filepath.Separator)) && path != chroot {
			return fmt.Errorf("%s cannot be archived from outside of chroot (%s)", name, chroot)
		}

		rel, err := filepath.Rel(chroot, path)
		if err != nil {
			return err
		}

		hdr := &hdrs[i]
		fileInfoHeader(rel, fi, hdr)

		if !a.options.modifiedEpoch.IsZero() {
			hdr.Modified = a.options.modifiedEpoch
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		switch {
		case hdr.Mode()&os.ModeSymlink != 0:
			err = a.createSymlink(path, fi, hdr)

		case hdr.Mode().IsDir():
			err = a.createDirectory(fi, hdr)

		default:
			if hdr.UncompressedSize64 > 0 {
				hdr.Method = a.options.method
			}

			err = a.createFile(ctx, path, fi, hdr)
			incOnSuccess(&a.entries, err)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func fileInfoHeader(name string, fi os.FileInfo, hdr *zip.FileHeader) {
	hdr.Name = filepath.ToSlash(name)
	hdr.UncompressedSize64 = uint64(fi.Size())
	hdr.Modified = fi.ModTime()
	hdr.SetMode(fi.Mode())

	if hdr.Mode().IsDir() {
		hdr.Name += "/"
	}
}

func (a *Archiver) createDirectory(fi os.FileInfo, hdr *zip.FileHeader) error {
	a.m.Lock()
	defer a.m.Unlock()

	_, err := a.createHeader(fi, hdr)
	incOnSuccess(&a.entries, err)
	return err
}

func (a *Archiver) createSymlink(path string, fi os.FileInfo, hdr *zip.FileHeader) error {
	a.m.Lock()
	defer a.m.Unlock()

	w, err := a.createHeader(fi, hdr)
	if err != nil {
		return err
	}

	link, err := os.Readlink(path)
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, link)
	incOnSuccess(&a.entries, err)
	return err
}

func (a *Archiver) createFile(ctx context.Context, path string, fi os.FileInfo, hdr *zip.FileHeader) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return a.compressFile(ctx, f, fi, hdr)
}

// compressFile pre-compresses the file first to a file from the filepool,
// making use of zip.CreateRaw. This allows for concurrent files to be
// compressed and then added to the zip file when ready.
// If no filepool file is available (when using a concurrency of 1) or the
// compressed file is larger than the uncompressed version, the file is moved
// to the zip file using the conventional zip.CreateHeader.
func (a *Archiver) compressFile(ctx context.Context, f *os.File, fi os.FileInfo, hdr *zip.FileHeader) error {
	return a.compressFileSimple(ctx, f, fi, hdr)
}

// compressFileSimple uses the conventional zip.createHeader. This differs from
// compressFile as it locks the zip _whilst_ compressing (if the method is not
// Store).
func (a *Archiver) compressFileSimple(ctx context.Context, f *os.File, fi os.FileInfo, hdr *zip.FileHeader) error {
	br := bufioReaderPool.Get().(*bufio.Reader)
	defer bufioReaderPool.Put(br)
	br.Reset(f)

	a.m.Lock()
	defer a.m.Unlock()

	w, err := a.createHeader(fi, hdr)
	if err != nil {
		return err
	}

	_, err = br.WriteTo(countWriter{w, &a.written, ctx})
	return err
}
