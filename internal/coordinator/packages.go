package coordinator

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"fabrenetes/internal/api"
)

func (s *Coordinator) ConfigurePackages(directory string) error {
	if directory == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return err
		}
		directory = filepath.Join(cache, "fabrenetes", "packages")
	}
	err := os.MkdirAll(directory, 0700)
	if err != nil {
		return fmt.Errorf("create package directory: %w", err)
	}
	s.packageDir = directory
	return nil
}

func (s *Coordinator) uploadPackage(w http.ResponseWriter, r *http.Request) {
	if s.packageDir == "" {
		writeError(w, http.StatusServiceUnavailable, "package storage is not configured")
		return
	}
	file, err := os.CreateTemp(s.packageDir, ".upload-*")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not store package")
		return
	}
	defer os.Remove(file.Name())
	digest := sha256.New()
	_, err = io.Copy(io.MultiWriter(file, digest), r.Body)
	closeErr := file.Close()
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read package upload")
		return
	}
	if closeErr != nil {
		writeError(w, http.StatusInternalServerError, "could not store package")
		return
	}
	archive, err := zip.OpenReader(file.Name())
	if err != nil {
		writeError(w, http.StatusBadRequest, "package must be a ZIP archive")
		return
	}
	err = archive.Close()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read stored package")
		return
	}
	id := hex.EncodeToString(digest.Sum(nil))
	err = os.Rename(file.Name(), filepath.Join(s.packageDir, id+".zip"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not store package")
		return
	}
	writeJSON(w, http.StatusCreated, api.Package{ID: id})
}

func (s *Coordinator) downloadPackage(w http.ResponseWriter, r *http.Request) {
	file, err := s.packagePath(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "package does not exist")
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	http.ServeFile(w, r, file)
}

func (s *Coordinator) packagePath(id string) (string, error) {
	digest, err := hex.DecodeString(id)
	if err != nil || len(digest) != sha256.Size || strings.ToLower(id) != id || s.packageDir == "" {
		return "", errors.New("package does not exist")
	}
	file := filepath.Join(s.packageDir, id+".zip")
	info, err := os.Stat(file)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("package does not exist")
	}
	return file, nil
}
