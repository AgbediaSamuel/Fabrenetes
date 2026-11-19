package coordinator

import (
	"errors"

	"fabrenetes/internal/api"
)

func normalizePartitioning(spec *api.ServiceSpec) error {
	if !spec.Stateful {
		if spec.Partitioning != "" || spec.Partitions != 0 {
			return errors.New("partitioning requires a stateful service")
		}
		return nil
	}
	if spec.Partitioning == "" {
		spec.Partitioning = "none"
	}
	if spec.Partitioning == "none" {
		if spec.Partitions == 0 {
			spec.Partitions = 1
		}
		if spec.Partitions != 1 {
			return errors.New("choose hash partitioning to use more than one partition")
		}
		return nil
	}
	if spec.Partitioning != "hash" || spec.Partitions < 1 {
		return errors.New("partitioning must be none or hash; hash requires a positive partition count")
	}
	return nil
}

func (s *Coordinator) validateStateReports(workerID string, reports []api.StateStatus) error {
	groups := make(map[string]*api.StateGroup)
	for _, service := range s.services {
		if service.State != nil {
			groups[service.State.ID] = service.State
		}
	}
	type identity struct {
		group string
		index int
	}
	seen := make(map[identity]bool)
	for _, report := range reports {
		id := identity{group: report.GroupID, index: report.Partition}
		if report.GroupID == "" || report.Partition < 0 || seen[id] {
			return errors.New("state reports require unique group and partition identities")
		}
		seen[id] = true
		group := groups[report.GroupID]
		if group == nil {
			continue
		}
		if report.Partition >= group.PartitionCount() {
			return errors.New("state report partition is not part of its group")
		}
		members := make(map[string]bool)
		for _, member := range group.Members {
			members[member.ID] = true
		}
		if !members[workerID] || (report.LeaderID != "" && !members[report.LeaderID]) {
			return errors.New("state reports must belong to this worker and name a group member as leader")
		}
		if report.Role != "leader" && report.Role != "follower" && report.Role != "candidate" {
			return errors.New("state report role must be leader, follower, or candidate")
		}
		if report.Available && (report.Role != "leader" || report.LeaderID != workerID || report.Term == 0) {
			return errors.New("only the current leader can report an available state partition")
		}
	}
	return nil
}

func (s *Coordinator) partitionStatus(service api.Service) []api.PartitionStatus {
	if service.State == nil {
		return nil
	}
	result := make([]api.PartitionStatus, service.State.PartitionCount())
	now := s.now()
	for index := range result {
		partition := api.PartitionStatus{Index: index, Status: "waiting"}
		reports := make(map[string]api.StateStatus)
		var highestTerm uint64
		for _, member := range service.State.Members {
			replica := api.StateReplicaStatus{WorkerID: member.ID, Role: "unknown", Status: "waiting"}
			worker, exists := s.workers[member.ID]
			if !exists || now.Sub(worker.LastSeen) >= s.timeout {
				replica.Status = "unreachable"
				partition.Status = "unavailable"
			} else {
				for _, report := range s.stateReports[member.ID] {
					if report.GroupID == service.State.ID && report.Partition == index {
						reports[member.ID] = report
						replica.Role = report.Role
						replica.Status = "ready"
						if report.Term > highestTerm {
							highestTerm = report.Term
						}
						break
					}
				}
			}
			partition.Replicas = append(partition.Replicas, replica)
		}
		if len(reports) > 0 {
			partition.Status = "unavailable"
		}
		conflict := false
		for _, report := range reports {
			if report.Term != highestTerm || report.LeaderID == "" {
				continue
			}
			if partition.LeaderID != "" && partition.LeaderID != report.LeaderID {
				conflict = true
			}
			partition.LeaderID = report.LeaderID
		}
		leader, reported := reports[partition.LeaderID]
		if !conflict && reported && leader.Term == highestTerm && leader.Available {
			partition.Status = "available"
		}
		if conflict {
			partition.LeaderID = ""
		}
		result[index] = partition
	}
	return result
}
