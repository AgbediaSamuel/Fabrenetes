package worker

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"syscall"

	"fabrenetes/internal/api"
)

type process struct {
	assignment api.Assignment
	command    *exec.Cmd
	done       chan error
	status     api.InstanceStatus
	restart    bool
}

type processes struct {
	workerID  string
	instances map[string]*process
}

func (p *processes) status() []api.InstanceStatus {
	statuses := make([]api.InstanceStatus, 0, len(p.instances))
	for _, child := range p.instances {
		if child.done != nil {
			select {
			case err := <-child.done:
				child.done = nil
				child.status.PID = 0
				child.status.Status = "failed"
				child.status.Error = "process exited"
				if err != nil {
					child.status.Error = err.Error()
				}
				child.restart = true
			default:
			}
		}
		statuses = append(statuses, child.status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return statuses
}

func (p *processes) apply(assignments []api.Assignment) {
	desired := make(map[string]bool, len(assignments))
	for _, assignment := range assignments {
		desired[assignment.ID] = true
	}
	for id, child := range p.instances {
		if !desired[id] {
			child.stop()
			delete(p.instances, id)
		}
	}
	for _, assignment := range assignments {
		child := p.instances[assignment.ID]
		if child == nil {
			child = &process{
				assignment: assignment,
				status:     api.InstanceStatus{ID: assignment.ID},
			}
			p.instances[assignment.ID] = child
			p.start(child)
		} else {
			if assignment.StopID != child.assignment.StopID {
				child.assignment.StopID = assignment.StopID
				if child.command != nil && child.done != nil {
					err := syscall.Kill(-child.command.Process.Pid, syscall.SIGKILL)
					if err != nil && err != syscall.ESRCH {
						fmt.Printf("Could not stop %s instance %s: %v\n", assignment.Service, assignment.ID, err)
					}
				}
			}
			if child.restart {
				child.stop()
				child.status.Restarts++
				p.start(child)
			}
		}
	}
}

func (p *processes) stop() {
	for _, child := range p.instances {
		child.stop()
	}
}

func (p *processes) start(child *process) {
	child.restart = false
	child.status.PID = 0
	child.status.Status = "failed"
	child.status.Error = ""
	port, err := p.availablePort(child)
	child.status.Port = 0
	if err != nil {
		child.status.Error = err.Error()
		fmt.Printf("Could not start %s: %v\n", child.assignment.Service, err)
		return
	}
	command := exec.Command(child.assignment.Command, child.assignment.Args...)
	command.Env = append(os.Environ(),
		"worker_id="+p.workerID,
		"service_name="+child.assignment.Service,
		"instance_id="+child.assignment.ID,
	)
	if port > 0 {
		command.Env = append(command.Env, fmt.Sprintf("service_port=%d", port))
	}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	err = command.Start()
	if err != nil {
		child.status.Error = err.Error()
		fmt.Printf("Could not start %s: %v\n", child.assignment.Service, err)
		return
	}
	child.command = command
	child.status.PID = command.Process.Pid
	child.status.Port = port
	child.status.Status = "running"
	child.done = make(chan error, 1)
	done := child.done
	go func() { done <- command.Wait() }()
	fmt.Printf("Started %s as process %d\n", child.assignment.Service, command.Process.Pid)
}

func (p *process) stop() {
	if p.command == nil {
		return
	}
	_ = syscall.Kill(-p.command.Process.Pid, syscall.SIGKILL)
	if p.done != nil {
		<-p.done
		p.done = nil
	}
	p.command = nil
}
