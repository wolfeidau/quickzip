package quickzip

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zip"
	"github.com/klauspost/compress/zstd"
	"github.com/saracen/zipextra"
	"golang.org/x/sync/errgroup"
)

var bufioWriterPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 32*1024)
	},
}

var (
	defaultDecompressor     = FlateDecompressor()
	defaultZstdDecompressor = ZstdDecompressor()
)

// Extractor is an opinionated Zip file extractor.
//
// Files are extracted in parallel. Only regular files, symlinks and directories
// are supported. Files can only be extracted to the specified chroot directory.
//
// Access permissions, ownership (unix) and modification times are preserved.
type Extractor struct {
	closer  io.Closer
	options extractorOptions
	zr      *zip.Reader
	written int64
	entries int64
	m       sync.Mutex
}

// NewExtractor opens a zip file and returns a new extractor.
//
// Close() should be called to close the extractor's underlying zip.Reader
// when done.
func NewExtractor(filename string, opts ...ExtractorOption) (*Extractor, error) {
	zr, err := zip.OpenReader(filename)
	if err != nil {
		return nil, err
	}

	return newExtractor(&zr.Reader, zr, opts)
}

// NewExtractor returns a new extractor, reading from the reader provided.
//
// The size of the archive should be provided.
//
// Unlike with NewExtractor(), calling Close() on the extractor is unnecessary.
func NewExtractorFromReader(r io.ReaderAt, size int64, opts ...ExtractorOption) (*Extractor, error) {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}

	return newExtractor(zr, nil, opts)
}

func newExtractor(r *zip.Reader, c io.Closer, opts []ExtractorOption) (*Extractor, error) {
	e := &Extractor{
		zr:     r,
		closer: c,
	}

	e.options.concurrency = runtime.GOMAXPROCS(0)
	for _, o := range opts {
		err := o(&e.options)
		if err != nil {
			return nil, err
		}
	}

	e.RegisterDecompressor(zip.Deflate, defaultDecompressor)
	e.RegisterDecompressor(zstd.ZipMethodWinZip, defaultZstdDecompressor)

	return e, nil
}

// RegisterDecompressor allows custom decompressors for a specified method ID.
// The common methods Store and Deflate are built in.
func (e *Extractor) RegisterDecompressor(method uint16, dcomp zip.Decompressor) {
	e.zr.RegisterDecompressor(method, dcomp)
}

// Files returns the file within the archive.
func (e *Extractor) Files() []*zip.File {
	return e.zr.File
}

// Close closes the underlying ZipReader.
func (e *Extractor) Close() error {
	if e.closer == nil {
		return nil
	}
	return e.closer.Close()
}

// Written returns how many bytes and entries have been written to disk.
// Written can be called whilst extraction is in progress.
func (e *Extractor) Written() (bytes, entries int64) {
	return atomic.LoadInt64(&e.written), atomic.LoadInt64(&e.entries)
}

// ExtractWithPathMapper extracts files from the zip archive, creating directories and symlinks as needed.
// It uses the provided pathFunc to map each zip.File to a destination path on the filesystem.
// The function returns an error if any part of the extraction fails.
func (e *Extractor) ExtractWithPathMapper(ctx context.Context, pathMapper func(file *zip.File) (string, error)) (err error) {
	limiter := make(chan struct{}, e.options.concurrency)

	wg, ctx := errgroup.WithContext(ctx)
	defer func() {
		if werr := wg.Wait(); werr != nil {
			err = werr
		}
	}()

	for i, file := range e.zr.File {
		if file.Mode()&irregularModes != 0 {
			continue
		}

		var path string
		path, err = pathMapper(file)
		if err != nil {
			return err
		}

		if err := os.MkdirAll(filepath.Dir(path), 0o777); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		switch {
		case file.Mode()&os.ModeSymlink != 0:
			// defer the creation of symlinks
			// this is to prevent a traversal vulnerability where a symlink is
			// first created and then files are additional extracted into it
			continue

		case file.Mode().IsDir():
			err = e.createDirectory(path, file)

		default:
			limiter <- struct{}{}

			gf := e.zr.File[i]
			wg.Go(func() error {
				defer func() { <-limiter }()
				err := e.createFile(ctx, path, gf)
				if err == nil {
					err = e.updateFileMetadata(path, gf)
				}
				return err
			})
		}
		if err != nil {
			return err
		}
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	// handle deferred symlink creation and update directory metadata
	// (otherwise modification dates are incorrect)
	for _, file := range e.zr.File {
		if file.Mode()&os.ModeSymlink == 0 && !file.Mode().IsDir() {
			continue
		}

		path, err := pathMapper(file)
		if err != nil {
			return err
		}

		if file.Mode()&os.ModeSymlink != 0 {
			if err := e.createSymlink(path, file); err != nil {
				return err
			}
			continue
		}

		err = e.updateFileMetadata(path, file)
		if err != nil {
			return err
		}
	}

	return nil
}

// Extract extracts files, creates symlinks and directories from the
// archive.
func (e *Extractor) Extract(ctx context.Context, chroot string) (err error) {
	if chroot, err = filepath.Abs(chroot); err != nil {
		return err
	}

	return e.ExtractWithPathMapper(ctx, func(file *zip.File) (string, error) {
		var path string
		path, err = filepath.Abs(filepath.Join(chroot, file.Name))
		if err != nil {
			return "", err
		}

		if !strings.HasPrefix(path, chroot+string(filepath.Separator)) && path != chroot {
			return "", fmt.Errorf("%s cannot be extracted outside of chroot (%s)", path, chroot)
		}

		return path, nil
	})
}

func (e *Extractor) createDirectory(path string, file *zip.File) error {
	err := os.Mkdir(path, 0777)
	if os.IsExist(err) {
		err = nil
	}
	incOnSuccess(&e.entries, err)
	return err
}

func (e *Extractor) createSymlink(path string, file *zip.File) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	r, err := file.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	name, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if err := os.Symlink(string(name), path); err != nil {
		return err
	}

	err = e.updateFileMetadata(path, file)
	incOnSuccess(&e.entries, err)

	return err
}

func (e *Extractor) createFile(ctx context.Context, path string, file *zip.File) (err error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	r, err := file.Open()
	if err != nil {
		return err
	}
	defer dclose(r, &err)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer dclose(f, &err)

	bw := bufioWriterPool.Get().(*bufio.Writer)
	defer bufioWriterPool.Put(bw)

	bw.Reset(countWriter{f, &e.written, ctx})
	if _, err = bw.ReadFrom(r); err != nil {
		return err
	}

	err = bw.Flush()
	incOnSuccess(&e.entries, err)

	return err
}

func (e *Extractor) updateFileMetadata(path string, file *zip.File) error {
	fields, err := zipextra.Parse(file.Extra)
	if err != nil {
		return err
	}

	if err := lchtimes(path, file.Mode(), time.Now(), file.Modified); err != nil {
		return err
	}

	if err := lchmod(path, file.Mode()); err != nil {
		return err
	}

	unixfield, ok := fields[zipextra.ExtraFieldUnixN]
	if !ok {
		return nil
	}

	unix, err := unixfield.InfoZIPNewUnix()
	if err != nil {
		return err
	}

	err = lchown(path, int(unix.Uid.Int64()), int(unix.Gid.Int64()))
	if err == nil {
		return nil
	}

	if e.options.chownErrorHandler == nil {
		return nil
	}

	e.m.Lock()
	defer e.m.Unlock()

	return e.options.chownErrorHandler(file.Name, err)
}
