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
	"strings"
	"sync"
	"time"
)

type NetworkType string

var (
	UDP NetworkType = "udp"
	TCP NetworkType = "tcp"
)

type gelfBackend struct {
	mu          *sync.Mutex
	conn        net.Conn
	networkType NetworkType
	addr        string
}

func NewGelfBackend(addr string) (Backend, error) {
	var err error
	var networkType NetworkType
	if strings.HasPrefix(addr, "tcp://") {
		networkType = TCP
		addr = strings.TrimPrefix(addr, "tcp://")
	} else if strings.HasPrefix(addr, "udp://") {
		networkType = UDP
		addr = strings.TrimPrefix(addr, "udp://")
	} else {
		return nil, fmt.Errorf("invalid protocol: %s", addr)
	}

	conn, err := net.Dial(string(networkType), addr)
	if err != nil {
		return nil, err
	}

	return &gelfBackend{
		mu:          &sync.Mutex{},
		conn:        conn,
		networkType: networkType,
		addr:        addr,
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

func (u *gelfBackend) tcpWritePack(pack []byte) error {
	pack = append(pack, '\x00')
	bytesLeft := len(pack)
	for {
		n, err := u.conn.Write(pack)
		if err != nil {
			return err
		}
		bytesLeft -= n
		if bytesLeft == 0 {
			break
		}
	}

	return nil
}

// tcpReconnect 重连直到成功
func (u *gelfBackend) tcpReconnect(interval time.Duration) {
	// 先关闭原来的连接
	_ = u.conn.Close()

	var connectCount int
	for {
		fmt.Printf("connect  %s://%s retrying %d\n", u.networkType, u.addr, connectCount)
		conn, err := net.Dial(string(u.networkType), u.addr)
		if err != nil {
			connectCount += 1
			time.Sleep(interval)
			continue
		}
		u.conn = conn
		return
	}
}

func (u *gelfBackend) udpWritePack(pack []byte) (err error) {
	b := make([]byte, 0, ChunkSize)
	buf := bytes.NewBuffer(b)
	chunkCount := numChunks(pack)
	if chunkCount > 255 {
		return fmt.Errorf("msg too large, would need %d chunks", chunkCount)
	}
	nChunks := uint8(chunkCount)
	if nChunks == 1 {
		n, err := u.conn.Write(pack)
		if err != nil {
			return err
		}
		if n != len(pack) {
			return fmt.Errorf("write (%d/%d)", n, len(pack))
		}
		return nil
	}
	// use random to get a unique message id
	msgId := make([]byte, 8)
	n, err := io.ReadFull(rand.Reader, msgId)
	if err != nil || n != 8 {
		return fmt.Errorf("rand.Reader: %d/%s", n, err)
	}

	bytesLeft := len(pack)
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
		chunk := pack[off : off+chunkLen]
		buf.Write(chunk)

		// write this chunk, and make sure the write was good
		n, err := u.conn.Write(buf.Bytes())
		if err != nil {
			return err
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

func (u *gelfBackend) SendMessage(m *GELFMessage) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	// tcp协议发送
	if u.networkType == TCP {
		for {
			if err := u.tcpWritePack(data); err != nil {
				u.tcpReconnect(time.Second)
				continue
			}
			return nil
		}
	}

	// udp协议发送
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

	return u.udpWritePack(buf.Bytes())
}

func (u *gelfBackend) Close() error {
	return u.conn.Close()
}

func (u *gelfBackend) LaunchConsume(func(message *GELFMessage) error) error {
	panic("implement me")
}
