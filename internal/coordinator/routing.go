package coordinator

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"fabrenetes/internal/api"
)

func (s *Coordinator) routeService(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.mu.Lock()
	service, exists := s.services[name]
	if !exists {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "service does not exist")
		return
	}
	var targets []api.Instance
	now := s.now()
	for _, instance := range service.Instances {
		worker, registered := s.workers[instance.WorkerID]
		if instance.Status == "running" && instance.PID > 0 && instance.Port > 0 && registered && now.Sub(worker.LastSeen) < s.timeout && net.ParseIP(worker.IP) != nil {
			targets = append(targets, instance)
		}
	}
	if len(targets) == 0 {
		s.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "service has no available application instances")
		return
	}
	instance := targets[s.nextInstance[name]%uint64(len(targets))]
	s.nextInstance[name]++
	worker := s.workers[instance.WorkerID]
	s.mu.Unlock()

	parts := strings.SplitN(r.URL.EscapedPath(), "/", 4)
	path := "/"
	if len(parts) == 4 {
		path += parts[3]
	}
	decoded, err := url.PathUnescape(path)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid application path")
		return
	}
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort(worker.IP, strconv.Itoa(instance.Port))}
	proxy := httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.URL.Path = decoded
			request.Out.URL.RawPath = path
			request.SetXForwarded()
		},
		ModifyResponse: func(response *http.Response) error {
			response.Header.Set("Fabrenetes-Worker", worker.ID)
			response.Header.Set("Fabrenetes-Instance", instance.ID)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.Header().Set("Fabrenetes-Worker", worker.ID)
			w.Header().Set("Fabrenetes-Instance", instance.ID)
			writeError(w, http.StatusBadGateway, "application instance is unavailable")
		},
	}
	proxy.ServeHTTP(w, r)
}
