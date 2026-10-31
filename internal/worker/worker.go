package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fabrenetes/internal/api"
)

type Config struct {
	ID                string
	Hostname          string
	CoordinatorURL    string
	HeartbeatInterval time.Duration
	RequestTimeout    time.Duration
}

func Run(ctx context.Context, config Config) error {
	baseURL, err := config.validate()
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: config.RequestTimeout}
	defer client.CloseIdleConnections()
	children := processes{workerID: config.ID, instances: make(map[string]*process)}
	defer children.stop()
	heartbeatURL := baseURL + "/workers/" + url.PathEscape(config.ID) + "/heartbeat"
	sessionID := ""
	disconnected := false
	defer fmt.Printf("Worker %s stopped\n", config.ID)

	for {
		var current api.Worker
		var response api.HeartbeatResponse
		registering := sessionID == ""
		if registering {
			err = post(ctx, client, baseURL+"/workers", api.RegisterRequest{
				ID: config.ID, Hostname: config.Hostname,
			}, http.StatusCreated, &current)
			if err == nil && current.SessionID == "" {
				err = errors.New("registration response has no session ID")
			}
		} else {
			err = post(ctx, client, heartbeatURL, api.HeartbeatRequest{
				SessionID: sessionID,
				Instances: children.status(),
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
