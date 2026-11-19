package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type Dictionary struct {
	url    string
	client *http.Client
}

func New(stateURL string) (*Dictionary, error) {
	if stateURL == "" {
		stateURL = os.Getenv("state_url")
	}
	if stateURL == "" {
		return nil, errors.New("state URL is required; set state_url or pass it to collections.New")
	}
	parsed, err := url.Parse(stateURL)
	if err != nil {
		return nil, fmt.Errorf("invalid state URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, errors.New("state URL must be an HTTP address without a query or fragment")
	}
	return &Dictionary{url: strings.TrimRight(parsed.String(), "/"), client: http.DefaultClient}, nil
}

func (d *Dictionary) Get(ctx context.Context, key string, destination any) (bool, error) {
	body, status, err := d.request(ctx, http.MethodGet, key, nil)
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound {
		return false, nil
	}
	if status < 200 || status >= 300 {
		return false, responseError(body, status)
	}
	err = json.Unmarshal(body, destination)
	if err != nil {
		return false, fmt.Errorf("could not decode dictionary value: %w", err)
	}
	return true, nil
}

func (d *Dictionary) Put(ctx context.Context, key string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	body, status, err := d.request(ctx, http.MethodPut, key, body)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return responseError(body, status)
	}
	return nil
}

func (d *Dictionary) Delete(ctx context.Context, key string) error {
	body, status, err := d.request(ctx, http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status < 200 || status >= 300 {
		return responseError(body, status)
	}
	return nil
}

func (d *Dictionary) request(ctx context.Context, method, key string, body []byte) ([]byte, int, error) {
	address := d.url + "/values?key=" + url.QueryEscape(key)
	request, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err = io.ReadAll(response.Body)
	return body, response.StatusCode, err
}

func responseError(body []byte, status int) error {
	var response struct {
		Error string `json:"error"`
	}
	err := json.Unmarshal(body, &response)
	if err == nil && response.Error != "" {
		return errors.New(response.Error)
	}
	return fmt.Errorf("state request failed: %d %s", status, http.StatusText(status))
}
