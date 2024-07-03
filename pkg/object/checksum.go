/*
 * JuiceFS, Copyright (C) 2020 Juicedata, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package object

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
	"strconv"
)

type algorithm string

const (
	checksumCrc32 algorithm = "Crc32c"
	checksumMd5   algorithm = "md5"
)

func (a algorithm) String() string {
	return string(a)
}

var crc32c = crc32.MakeTable(crc32.Castagnoli)

func generateChecksum(in io.ReadSeeker, algorithmType algorithm) string {
	switch algorithmType {
	case checksumCrc32:
		if b, ok := in.(*bytes.Reader); ok {
			v := reflect.ValueOf(b)
			data := v.Elem().Field(0).Bytes()
			return strconv.Itoa(int(crc32.Update(0, crc32c, data)))
		}
		var hash uint32
		crcBuffer := bufPool.Get().(*[]byte)
		defer bufPool.Put(crcBuffer)
		defer func() { _, _ = in.Seek(0, io.SeekStart) }()
		for {
			n, err := in.Read(*crcBuffer)
			hash = crc32.Update(hash, crc32c, (*crcBuffer)[:n])
			if err != nil {
				if err != io.EOF {
					return ""
				}
				break
			}
		}
		return strconv.Itoa(int(hash))
	case checksumMd5:
		w := md5.New()
		if b, ok := in.(*bytes.Reader); ok {
			v := reflect.ValueOf(b)
			data := v.Elem().Field(0).Bytes()
			io.WriteString(w, string(data))
			return string(w.Sum(nil))
		}
		crcBuffer := bufPool.Get().(*[]byte)
		defer bufPool.Put(crcBuffer)
		defer func() { _, _ = in.Seek(0, io.SeekStart) }()
		for {
			n, err := in.Read(*crcBuffer)
			io.WriteString(w, string((*crcBuffer)[:n]))
			if err != nil {
				if err != io.EOF {
					return ""
				}
				break
			}
		}
		return base64.StdEncoding.EncodeToString(w.Sum(nil))
	default:
		return ""
	}
}

type checksumReader struct {
	io.ReadCloser
	expected uint32
	checksum uint32
}

func (c *checksumReader) Read(buf []byte) (n int, err error) {
	n, err = c.ReadCloser.Read(buf)
	c.checksum = crc32.Update(c.checksum, crc32c, buf[:n])
	if err == io.EOF && c.checksum != c.expected {
		return 0, fmt.Errorf("verify checksum failed: %d != %d", c.checksum, c.expected)
	}
	return
}

func verifyChecksum(in io.ReadCloser, checksum string) io.ReadCloser {
	if checksum == "" {
		return in
	}
	expected, err := strconv.Atoi(checksum)
	if err != nil {
		fmt.Printf("invalid crc32c: %s\n", checksum)
		return in
	}
	return &checksumReader{in, uint32(expected), 0}
}
