package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"

	"fabrenetes/internal/api"
)

func uploadPackage(ctx context.Context, address, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	endpoint, err := url.JoinPath(address, "packages")
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, file)
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/zip")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	var uploaded api.Package
	err = readCoordinatorResponse(response, &uploaded)
	if err != nil {
		return "", err
	}
	if uploaded.ID == "" {
		return "", errors.New("package upload returned no package ID")
	}
	return uploaded.ID, nil
}
