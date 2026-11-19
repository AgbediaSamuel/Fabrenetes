package worker

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type packageStore struct {
	mu      sync.Mutex
	baseURL string
	dir     string
	client  *http.Client
}

type preparation struct {
	directory string
	err       error
}

func (s *packageStore) prepare(ctx context.Context, id string) (string, error) {
	archive, err := s.archive(ctx, id)
	if err != nil {
		return "", err
	}
	instances := filepath.Join(s.dir, "instances")
	err = os.MkdirAll(instances, 0700)
	if err != nil {
		return "", err
	}
	directory, err := os.MkdirTemp(instances, "instance-")
	if err != nil {
		return "", err
	}
	err = extractPackage(ctx, archive, directory)
	if err != nil {
		os.RemoveAll(directory)
		return "", err
	}
	return directory, nil
}

func (s *packageStore) archive(ctx context.Context, id string) (string, error) {
	digest, err := hex.DecodeString(id)
	if err != nil || len(digest) != sha256.Size || strings.ToLower(id) != id {
		return "", fmt.Errorf("invalid package ID %q", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err = ctx.Err()
	if err != nil {
		return "", err
	}
	directory := filepath.Join(s.dir, "packages")
	err = os.MkdirAll(directory, 0700)
	if err != nil {
		return "", err
	}
	archive := filepath.Join(directory, id+".zip")
	_, err = os.Stat(archive)
	if err == nil {
		return archive, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/packages/"+id, nil)
	if err != nil {
		return "", err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("download package: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download package: HTTP %d", response.StatusCode)
	}
	file, err := os.CreateTemp(directory, "download-")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(file, hash), response.Body)
	if err != nil {
		return "", fmt.Errorf("download package: %w", err)
	}
	err = file.Close()
	if err != nil {
		return "", err
	}
	if hex.EncodeToString(hash.Sum(nil)) != id {
		return "", errors.New("downloaded package does not match its checksum")
	}
	err = os.Rename(file.Name(), archive)
	if err != nil {
		return "", err
	}
	return archive, nil
}

func extractPackage(ctx context.Context, archive, directory string) error {
	reader, err := zip.OpenReader(archive)
	if err != nil {
		return fmt.Errorf("open package: %w", err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		if !filepath.IsLocal(file.Name) || (!file.Mode().IsRegular() && !file.Mode().IsDir()) {
			return fmt.Errorf("unsupported package path %q", file.Name)
		}
	}
	for _, file := range reader.File {
		err = ctx.Err()
		if err != nil {
			return err
		}
		target := filepath.Join(directory, file.Name)
		if file.Mode().IsDir() {
			err = os.MkdirAll(target, 0700)
		} else {
			err = extractFile(ctx, file, target)
		}
		if err != nil {
			return fmt.Errorf("extract %q: %w", file.Name, err)
		}
	}
	return nil
}

func extractFile(ctx context.Context, entry *zip.File, target string) error {
	err := os.MkdirAll(filepath.Dir(target), 0700)
	if err != nil {
		return err
	}
	source, err := entry.Open()
	if err != nil {
		return err
	}
	defer source.Close()
	file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, entry.Mode().Perm())
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, packageReader{ctx: ctx, reader: source})
	if err != nil {
		return err
	}
	return file.Close()
}

type packageReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r packageReader) Read(data []byte) (int, error) {
	err := r.ctx.Err()
	if err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}
