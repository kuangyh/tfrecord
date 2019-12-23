// Package tfrecord is an obvious tfrecord IO implementation
//
// Format spec: https://www.tensorflow.org/tutorials/load_data/tfrecord,
// assume all numbers are little-endian although not actually defined in spec.
package tfrecord

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	crcMagicNum = 0xa282ead8

	lengthSize = 8
	crcSize    = 4
	headerSize = lengthSize + crcSize
	footerSize = crcSize
)

// ErrChecksum is error returned when TFRecord content doesn't pass checksum.
// It indicates data corruption or wrong file format.
var ErrChecksum = errors.New("checksum error in TFRecord")

// see TFREcord spec.
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func checksum(p []byte) uint32 {
	crc := crc32.Checksum(p, crc32Table)
	return ((crc >> 15) | (crc << 17)) + crcMagicNum
}

// Iterator iterates TFRecords through an io.Reader
type Iterator struct {
	r            io.Reader
	checkDataCRC bool

	preBuf []byte
	value  []byte
	err    error
}

// NewIterator creates a Iterator. Iterator pre-allocates and reuse buffer to avoid frequent buffer allocation,
// bufSize should be set to upper-bound of expected common record size. when checkDataCRC is true, check CRC of
// data content, this is the recommend setup because checking CRC of data won't be performance bottleneck in most cases.
func NewIterator(r io.Reader, bufSize int64, checkDataCRC bool) *Iterator {
	var buf []byte
	if bufSize > 0 {
		buf = make([]byte, bufSize)
	}
	return &Iterator{
		r:            r,
		checkDataCRC: checkDataCRC,
		preBuf:       buf,
	}
}

// Next reads in next record from underlying reader
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	withError := func(err error) bool {
		it.err = err
		return false
	}

	it.value = nil
	header := [headerSize]byte{}
	if _, err := io.ReadFull(it.r, header[:]); err != nil {
		if err == io.EOF {
			return false
		}
		return withError(err)
	}
	recordLen := binary.LittleEndian.Uint64(header[:lengthSize])
	lenCRC := binary.LittleEndian.Uint32(header[lengthSize:])
	if crc := checksum(header[:lengthSize]); crc != lenCRC {
		return withError(ErrChecksum)
	}

	var record []byte
	if recordLen > uint64(len(it.preBuf)) {
		record = make([]byte, recordLen)
	} else {
		record = it.preBuf[:recordLen]
	}
	if _, err := io.ReadFull(it.r, record); err != nil {
		return withError(err)
	}
	var footer [footerSize]byte
	if _, err := io.ReadFull(it.r, footer[:]); err != nil {
		return withError(err)
	}
	if it.checkDataCRC {
		dataCRC := binary.LittleEndian.Uint32(footer[:])
		if crc := checksum(record); crc != dataCRC {
			return withError(ErrChecksum)
		}
	}
	it.value = record
	return true
}

// Err returns any error stopping Next(), io.EOF is not considered error
func (it *Iterator) Err() error {
	return it.err
}

// Value returns the current value, returns nil when iterator not in valid state
func (it *Iterator) Value() []byte {
	return it.value
}

// NewWriter creates a TFRecord writer on top of w
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Writer implements io.Writer that writes TFRecord
type Writer struct {
	w io.Writer
}

// Write implements io.Write
func (w *Writer) Write(record []byte) (n int, err error) {
	header := [headerSize]byte{}
	binary.LittleEndian.PutUint64(header[:lengthSize], uint64(len(record)))
	binary.LittleEndian.PutUint32(header[lengthSize:], checksum(header[:lengthSize]))
	if _, err := w.w.Write(header[:]); err != nil {
		return 0, err
	}

	if _, err := w.w.Write(record); err != nil {
		return 0, err
	}
	var footer [footerSize]byte
	binary.LittleEndian.PutUint32(footer[:], checksum(record))
	if _, err := w.w.Write(footer[:]); err != nil {
		return 0, err
	}
	return len(record), nil
}
