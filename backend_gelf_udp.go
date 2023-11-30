package graylog

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

type udpBackend struct {
	mu   *sync.Mutex
	conn net.Conn
}

func NewUdpBackend(addr string) (Backend, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	return &udpBackend{
		mu:   &sync.Mutex{},
		conn: conn,
	}, nil
}

// Used to control GELF chunking.  Should be less than (MTU - len(UDP header)).
const (
	ChunkSize        = 1420
	chunkedHeaderLen = 12
	chunkedDataLen   = ChunkSize - chunkedHeaderLen
)

var (
	magicChunked = []byte{0x1e, 0x0f}
)

// numChunks returns the number of GELF chunks necessary to transmit
// the given compressed buffer.
func numChunks(b []byte) int {
	lenB := len(b)
	if lenB <= ChunkSize {
		return 1
	} else if len(b)%chunkedDataLen == 0 {
		return len(b) / chunkedDataLen
	} else {
		return len(b)/chunkedDataLen + 1
	}
}

func (u *udpBackend) write(bs []byte) (err error) {
	b := make([]byte, 0, ChunkSize)
	buf := bytes.NewBuffer(b)
	chunkCount := numChunks(bs)
	if chunkCount > 255 {
		return fmt.Errorf("msg too large, would need %d chunks", chunkCount)
	}
	nChunks := uint8(chunkCount)
	if nChunks == 1 {
		n, err := u.conn.Write(bs)
		if err != nil {
			return err
		}
		if n != len(bs) {
			return fmt.Errorf("write (%d/%d)", n, len(bs))
		}
		return nil
	}
	// use random to get a unique message id
	msgId := make([]byte, 8)
	n, err := io.ReadFull(rand.Reader, msgId)
	if err != nil || n != 8 {
		return fmt.Errorf("rand.Reader: %d/%s", n, err)
	}

	bytesLeft := len(bs)
	for i := uint8(0); i < nChunks; i++ {
		buf.Reset()
		// manually write header.  Don't care about
		// host/network byte order, because the spec only
		// deals in individual bytes.
		buf.Write(magicChunked) //magic
		buf.Write(msgId)
		buf.WriteByte(i)
		buf.WriteByte(nChunks)
		// slice out our chunk from zBytes
		chunkLen := chunkedDataLen
		if chunkLen > bytesLeft {
			chunkLen = bytesLeft
		}
		off := int(i) * chunkedDataLen
		chunk := bs[off : off+chunkLen]
		buf.Write(chunk)

		// write this chunk, and make sure the write was good
		n, err := u.conn.Write(buf.Bytes())
		if err != nil {
			return fmt.Errorf("write (chunk %d/%d): %s", i, nChunks, err)
		}
		if n != len(buf.Bytes()) {
			return fmt.Errorf("write len: (chunk %d/%d) (%d/%d)", i, nChunks, n, len(buf.Bytes()))
		}

		bytesLeft -= chunkLen
	}

	if bytesLeft != 0 {
		return fmt.Errorf("error: %d bytes left after sending", bytesLeft)
	}
	return nil
}

func (u *udpBackend) SendMessage(m *GELFMessage) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, flate.BestSpeed)
	if err != nil {
		return err
	}

	if _, err = zw.Write(data); err != nil {
		return err
	}
	// ensure all data is written
	_ = zw.Close()

	return u.write(buf.Bytes())
}

func (u *udpBackend) Close() error {
	return u.conn.Close()
}

func (u *udpBackend) LaunchConsumeSync(func(message *GELFMessage) error) error {
	panic("implement me")
}
