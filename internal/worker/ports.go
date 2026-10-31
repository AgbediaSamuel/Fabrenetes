package worker

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
)

func (p *processes) availablePort(child *process) (int, error) {
	first := child.assignment.Port
	if first == 0 {
		return 0, nil
	}
	used := make(map[int]bool)
	for _, other := range p.instances {
		if other != child && other.status.PID > 0 && other.status.Port > 0 {
			used[other.status.Port] = true
		}
	}
	previous := child.status.Port
	if previous > 0 && !used[previous] {
		available, err := portAvailable(previous)
		if err != nil {
			return 0, err
		}
		if available {
			return previous, nil
		}
	}
	for port := first; port <= 65535; port++ {
		if used[port] || port == previous {
			continue
		}
		available, err := portAvailable(port)
		if err != nil {
			return 0, err
		}
		if available {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no TCP port available at or above %d", first)
}

func portAvailable(port int) (bool, error) {
	listener, err := net.Listen("tcp4", net.JoinHostPort("", strconv.Itoa(port)))
	if errors.Is(err, syscall.EADDRINUSE) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	err = listener.Close()
	return err == nil, err
}
