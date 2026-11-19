package state

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type stream struct {
	address     string
	groupID     string
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
	mu          sync.Mutex
	open        map[*trackedConnection]bool
}

func (s *stream) Accept() (net.Conn, error) {
	select {
	case connection := <-s.connections:
		return connection, nil
	case <-s.done:
		return nil, net.ErrClosed
	}
}

func (s *stream) Addr() net.Addr {
	return streamAddress(s.address)
}

func (s *stream) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		connections := make([]*trackedConnection, 0, len(s.open))
		for connection := range s.open {
			connections = append(connections, connection)
		}
		s.mu.Unlock()
		for _, connection := range connections {
			connection.Close()
		}
	})
	return nil
}

func (s *stream) track(connection net.Conn) (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		connection.Close()
		return nil, net.ErrClosed
	default:
	}
	if s.open == nil {
		s.open = make(map[*trackedConnection]bool)
	}
	tracked := &trackedConnection{Conn: connection, stream: s}
	s.open[tracked] = true
	return tracked, nil
}

func (s *stream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	connection, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	connection.SetDeadline(time.Now().Add(timeout))
	endpoint := "/state/" + url.PathEscape(s.groupID) + "/raft"
	_, err = fmt.Fprintf(connection, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", endpoint, address)
	if err != nil {
		connection.Close()
		return nil, err
	}
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		connection.Close()
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		connection.Close()
		return nil, fmt.Errorf("connect to state group: %s", response.Status)
	}
	connection.SetDeadline(time.Time{})
	return s.track(&bufferedConnection{Conn: connection, reader: reader})
}

func (m *Manager) connect(w http.ResponseWriter, request *http.Request) {
	current := m.find(request.PathValue("id"))
	if current == nil {
		writeError(w, http.StatusNotFound, "state group does not exist")
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		writeError(w, http.StatusInternalServerError, "state transport requires an HTTP connection")
		return
	}
	connection, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	connection.SetDeadline(time.Time{})
	_, err = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
	if err == nil {
		err = buffered.Flush()
	}
	if err != nil {
		connection.Close()
		return
	}
	wrapped, err := current.stream.track(&bufferedConnection{Conn: connection, reader: buffered.Reader})
	if err != nil {
		return
	}
	select {
	case current.stream.connections <- wrapped:
	case <-current.stream.done:
		connection.Close()
	}
}

type bufferedConnection struct {
	net.Conn
	reader *bufio.Reader
}

type trackedConnection struct {
	net.Conn
	stream *stream
	once   sync.Once
}

func (c *trackedConnection) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.stream.mu.Lock()
		delete(c.stream.open, c)
		c.stream.mu.Unlock()
	})
	return err
}

func (c *bufferedConnection) Read(buffer []byte) (int, error) {
	return c.reader.Read(buffer)
}

type streamAddress string

func (address streamAddress) Network() string { return "tcp" }
func (address streamAddress) String() string  { return string(address) }
