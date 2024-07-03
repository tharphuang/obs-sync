package object

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"obs-sync/models"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	dirSuffix = "/"
)

var TryCFR bool // try copy_file_range

type filestore struct {
	DefaultObjectStorage
	root string
}

type onlyWriter struct {
	io.Writer
}

func (f *filestore) SetCheckSumKey(meta string) error {
	return notSupported
}

func (f *filestore) IsSetMd5(flag bool) error {
	return notSupported
}

func (f *filestore) String() string {
	if runtime.GOOS == "windows" {
		return "file:///" + f.root
	}
	return "file://" + f.root
}

func (f *filestore) path(key string) string {
	if strings.HasSuffix(f.root, dirSuffix) {
		return filepath.Join(f.root, key)
	}
	return f.root + "/" + key //Note: 这里需要在路径和key之间插入目录分割符
}

func (f *filestore) Head(key string) (Object, error) {
	p := f.path(key)

	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	if fi.IsDir() {
		size = 0
	}
	return &obj{
		key,
		size,
		fi.ModTime(),
		fi.IsDir(),
	}, nil
}

func (f *filestore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	p := f.path(key)

	localFile, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	fileInfo, err := localFile.Stat()
	if err != nil {
		localFile.Close()
		return nil, err
	}
	if fileInfo.IsDir() {
		localFile.Close()
		return ioutil.NopCloser(bytes.NewBuffer([]byte{})), nil
	}

	if off > 0 {
		if _, err = localFile.Seek(off, 0); err != nil {
			localFile.Close()
			return nil, err
		}
	}
	if limit > 0 {
		defer localFile.Close()
		buf := make([]byte, limit)
		if _, err = localFile.Read(buf); err != nil {
			return nil, err
		} else {
			return ioutil.NopCloser(bytes.NewBuffer(buf)), nil
		}
	}
	return localFile, nil
}

func (f *filestore) Put(key string, in io.Reader, _ models.CannedACLType) error {
	p := f.path(key)

	if strings.HasSuffix(key, dirSuffix) || key == "" && strings.HasSuffix(f.root, dirSuffix) {
		return os.MkdirAll(p, os.FileMode(0755))
	}

	tmp := filepath.Join(filepath.Dir(p), "."+filepath.Base(p)+".tmp"+strconv.Itoa(rand.Int()))
	localFile, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(p), os.FileMode(0755)); err != nil {
			return err
		}
		localFile, err = os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	}
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()

	if TryCFR {
		_, err = io.Copy(localFile, in)
	} else {
		buf := bufPool.Get().(*[]byte)
		defer bufPool.Put(buf)
		_, err = io.CopyBuffer(onlyWriter{localFile}, in, *buf)
	}
	if err != nil {
		_ = localFile.Close()
		return err
	}
	err = localFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(tmp, p)
	return err
}

func (f *filestore) Copy(dst, src string) error {
	r, err := f.Get(src, 0, -1)
	if err != nil {
		return err
	}
	defer r.Close()
	return f.Put(dst, r, "")
}

func (f *filestore) Delete(key string) error {
	err := os.Remove(f.path(key))
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return err
}

// walk recursively descends path, calling w.
func walk(path string, info os.FileInfo, walkFn filepath.WalkFunc) error {
	err := walkFn(path, info, nil)
	if err != nil {
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	infos, err := readDirSorted(path)
	if err != nil {
		return walkFn(path, info, err)
	}

	for _, fi := range infos {
		p := filepath.Join(path, fi.Name())
		err = walk(p, fi, walkFn)
		if err != nil && err != filepath.SkipDir {
			return err
		}
	}
	return nil
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. All errors that arise visiting files
// and directories are filtered by walkFn. The files are walked in lexical
// order, which makes the output deterministic but means that for very
// large directories Walk can be inefficient.
// Walk always follow symbolic links.
func Walk(root string, walkFn filepath.WalkFunc) error {
	info, err := os.Stat(root)
	if err != nil {
		err = walkFn(root, nil, err)
	} else {
		err = walk(root, info, walkFn)
	}
	if err == filepath.SkipDir {
		return nil
	}
	return err
}

type mInfo struct {
	name string
	os.FileInfo
}

func (m *mInfo) Name() string {
	return m.name
}

// readDirSorted reads the directory named by dirname and returns
// a sorted list of directory entries.
func readDirSorted(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fis, err := f.Readdir(-1)
	for i, fi := range fis {
		if !fi.IsDir() && !fi.Mode().IsRegular() {
			// follow symlink
			f, err := os.Stat(filepath.Join(dirname, fi.Name()))
			if err != nil {
				logger.Warn().Msgf("skip broken symlink %s", filepath.Join(dirname, fi.Name()))
				continue
			}
			fi = &mInfo{fi.Name(), f}
			fis[i] = fi
		}
		if fi.IsDir() {
			fis[i] = &mInfo{fi.Name() + dirSuffix, fi}
		}
	}
	sort.Slice(fis, func(i, j int) bool { return fis[i].Name() < fis[j].Name() })
	return fis, err
}

func (f *filestore) List(prefix, marker string, limit int64) ([]Object, error) {
	return nil, notSupported
}

func (f *filestore) GetObjectAcl(key string) (models.CannedACLType, error) {
	return "", nil
}

func (f *filestore) ListAll(prefix, marker string) (<-chan Object, error) {
	listed := make(chan Object, 10240)
	go func() {
		var walkRoot string
		if strings.HasSuffix(f.root, dirSuffix) {
			walkRoot = f.root
		} else {
			// If the root is not ends with `/`, we'll list the directory root resides.
			walkRoot = path.Dir(f.root)
		}

		_ = Walk(walkRoot, func(path string, info os.FileInfo, err error) error {
			if runtime.GOOS == "windows" {
				path = strings.Replace(path, "\\", "/", -1)
			}

			if err != nil {
				// 跳过软链接
				if fi, err1 := os.Lstat(path); err1 == nil && fi.Mode()&os.ModeSymlink != 0 {
					logger.Warn().Msgf("skip unreachable symlink: %s (%s)", path, err)
					return nil
				}
				if os.IsNotExist(err) {
					logger.Warn().Msgf("skip not exist file or directory: %s", path)
					return nil
				}
				listed <- nil
				logger.Error().Msgf("list %s: %s", path, err)
				return err
			}

			if !strings.HasPrefix(path, f.root) {
				if info.IsDir() && path != walkRoot {
					return filepath.SkipDir
				}
				return nil
			}

			key := path[len(f.root):]
			if !strings.HasPrefix(key, prefix) || (marker != "" && key <= marker) || key == "" {
				if info.IsDir() && !strings.HasPrefix(prefix, key) && !strings.HasPrefix(marker, key) {
					return filepath.SkipDir
				}
				return nil
			}
			owner, group := getOwnerGroup(info)
			if key[0] == '/' {
				key = key[1:] //Note: 这里的key包含了'/'需要去除
			}
			localFile := &file{
				obj{
					key,
					info.Size(),
					info.ModTime(),
					info.IsDir(),
				},
				owner,
				group,
				info.Mode(),
			}
			if info.IsDir() {
				localFile.size = 0
				if localFile.key != "" || !strings.HasSuffix(f.root, dirSuffix) {
					localFile.key += dirSuffix
				}
			}
			listed <- localFile
			return nil
		})
		close(listed)
	}()
	return listed, nil
}

func (f *filestore) Chtimes(path string, mtime time.Time) error {
	p := f.path(path)
	return os.Chtimes(p, mtime, mtime)
}

func (f *filestore) Chmod(path string, mode os.FileMode) error {
	p := f.path(path)
	return os.Chmod(p, mode)
}

func (f *filestore) Chown(path string, owner, group string) error {
	p := f.path(path)
	uid := lookupUser(owner)
	gid := lookupGroup(group)
	return os.Chown(p, uid, gid)
}

func newDisk(root, accesskey, secretkey string) (ObjectStorage, error) {
	// For Windows, the path looks like /C:/a/b/c/
	if runtime.GOOS == "windows" && strings.HasPrefix(root, "/") {
		root = root[1:]
	}
	if strings.HasSuffix(root, dirSuffix) {
		//logger.Debug().Msgf("Ensure directory %s", root)
		if err := os.MkdirAll(root, 0755); err != nil {
			return nil, fmt.Errorf("Creating directory %s failed: %q", root, err)
		}
	} else {
		dir := path.Dir(root)
		//logger.Debug().Msgf("Ensure directory %s", dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("Creating directory %s failed: %q", dir, err)
		}
	}
	return &filestore{root: root}, nil
}

func init() {
	Register(models.File, newDisk)
}
