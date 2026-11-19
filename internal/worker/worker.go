package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fabrenetes/internal/api"
	"fabrenetes/internal/state"
)

type Config struct {
	ID                string
	Hostname          string
	CoordinatorURL    string
	HeartbeatInterval time.Duration
	RequestTimeout    time.Duration
	DataDir           string
	StateListen       string
	StateAddress      string
}

func Run(ctx context.Context, config Config) error {
	baseURL, err := config.validate()
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: config.RequestTimeout}
	defer client.CloseIdleConnections()
	directory := config.DataDir
	if directory == "" {
		cache, err := os.UserConfigDir()
		if err != nil {
			return fmt.Errorf("find worker data directory: %w", err)
		}
		identity := sha256.Sum256([]byte(config.ID))
		directory = filepath.Join(cache, "fabrenetes", "workers", fmt.Sprintf("%x", identity))
	}
	packages := &packageStore{baseURL: baseURL, dir: directory, client: &http.Client{}}
	defer packages.client.CloseIdleConnections()
	var dictionaries *state.Manager
	var stateResult <-chan error
	stateURL := ""
	stateAddress := ""
	if config.StateListen != "" {
		listener, err := net.Listen("tcp", config.StateListen)
		if err != nil {
			return fmt.Errorf("listen for collections: %w", err)
		}
		defer listener.Close()
		stateAddress = config.StateAddress
		if stateAddress == "" {
			stateAddress = listener.Addr().String()
		}
		dictionaries, err = state.New(config.ID, filepath.Join(directory, "state"), stateAddress)
		if err != nil {
			return err
		}
		defer dictionaries.Close()
		server := &http.Server{Handler: dictionaries.Handler(), ReadHeaderTimeout: 5 * time.Second}
		defer server.Close()
		result := make(chan error, 1)
		go func() { result <- server.Serve(listener) }()
		stateResult = result
		host, port, _ := net.SplitHostPort(listener.Addr().String())
		ip := net.ParseIP(host)
		if ip != nil && ip.IsUnspecified() {
			host = "127.0.0.1"
		}
		stateURL = "http://" + net.JoinHostPort(host, port)
		fmt.Println("Collections listening on", listener.Addr())
	}
	children := processes{workerID: config.ID, instances: make(map[string]*process), ctx: ctx, packages: packages, stateURL: stateURL}
	defer children.stop()
	heartbeatURL := baseURL + "/workers/" + url.PathEscape(config.ID) + "/heartbeat"
	sessionID := ""
	disconnected := false
	defer fmt.Printf("Worker %s stopped\n", config.ID)

	for {
		select {
		case err := <-stateResult:
			return fmt.Errorf("collections server stopped: %w", err)
		default:
		}
		var current api.Worker
		var response api.HeartbeatResponse
		registering := sessionID == ""
		if registering {
			err = post(ctx, client, baseURL+"/workers", api.RegisterRequest{
				ID: config.ID, Hostname: config.Hostname, StateAddress: stateAddress,
			}, http.StatusCreated, &current)
			if err == nil && current.SessionID == "" {
				err = errors.New("registration response has no session ID")
			}
		} else {
			var stateReports []api.StateStatus
			if dictionaries != nil {
				statusCtx, cancel := context.WithTimeout(ctx, config.RequestTimeout)
				stateReports = dictionaries.Status(statusCtx)
				cancel()
			}
			err = post(ctx, client, heartbeatURL, api.HeartbeatRequest{
				SessionID: sessionID,
				Instances: children.status(),
				State:     stateReports,
			}, http.StatusOK, &response)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			var responseErr *responseError
			if errors.As(err, &responseErr) {
				if !registering && responseErr.code == http.StatusNotFound {
					fmt.Printf("Worker %s lost registration; registering again\n", config.ID)
					sessionID = ""
					continue
				}
				if responseErr.code == http.StatusConflict {
					return fmt.Errorf("worker %q session was replaced: %w", config.ID, err)
				}
				if responseErr.code < 500 && responseErr.code != http.StatusRequestTimeout && responseErr.code != http.StatusTooManyRequests {
					return fmt.Errorf("coordinator rejected worker %q: %w", config.ID, err)
				}
			}
			if !disconnected {
				fmt.Printf("Worker %s cannot reach coordinator: %v; retrying\n", config.ID, err)
				disconnected = true
			}
		} else {
			if disconnected {
				fmt.Printf("Worker %s reconnected\n", config.ID)
				disconnected = false
			}
			if registering {
				sessionID = current.SessionID
				fmt.Printf("Worker %s registered with %s\n", config.ID, baseURL)
			} else {
				if dictionaries != nil {
					groups := make([]api.StateGroup, 0)
					for _, assignment := range response.Assignments {
						if assignment.State != nil {
							groups = append(groups, *assignment.State)
						}
					}
					err = dictionaries.Apply(groups)
					if err != nil {
						return fmt.Errorf("configure collections: %w", err)
					}
				}
				children.apply(response.Assignments)
			}
		}
		if !wait(ctx, config.HeartbeatInterval) {
			return nil
		}
	}
}

func (config Config) validate() (string, error) {
	if !api.ValidName(config.ID) || strings.TrimSpace(config.Hostname) == "" {
		return "", errors.New("valid worker ID and hostname are required")
	}
	if config.HeartbeatInterval <= 0 || config.RequestTimeout <= 0 {
		return "", errors.New("heartbeat interval and request timeout must be positive")
	}
	if config.StateAddress != "" && config.StateListen == "" {
		return "", errors.New("state-address requires state-listen")
	}
	baseURL, err := url.Parse(config.CoordinatorURL)
	if err != nil || baseURL.Host == "" ||
		(baseURL.Scheme != "http" && baseURL.Scheme != "https") ||
		baseURL.RawQuery != "" || baseURL.ForceQuery || baseURL.Fragment != "" {
		return "", errors.New("coordinator URL must be an HTTP or HTTPS base URL without a query or fragment")
	}
	return strings.TrimRight(baseURL.String(), "/"), nil
}

type responseError struct {
	code    int
	message string
}

func (err *responseError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", err.code, err.message)
}

func post(ctx context.Context, client *http.Client, endpoint string, body any, expectedStatus int, result any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != expectedStatus {
		var responseBody api.ErrorResponse
		err = json.Unmarshal(data, &responseBody)
		if err != nil || responseBody.Error == "" {
			responseBody.Error = response.Status
		}
		return &responseError{code: response.StatusCode, message: responseBody.Error}
	}
	return json.Unmarshal(data, result)
}

func wait(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
