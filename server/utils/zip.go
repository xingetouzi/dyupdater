package utils

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"path/filepath"
)

func safeJoinPath(a, b string) string {
	if a == "" {
		return b
	}
	return filepath.Join(a, b)
}

func addFiles(w *zip.Writer, basePath, baseInZip string) error {
	// Open the Directory
	files, err := ioutil.ReadDir(basePath)
	if err != nil {
		return err
	}
	for _, file := range files {
		realPath := safeJoinPath(basePath, file.Name())
		realPathInZip := safeJoinPath(baseInZip, file.Name())
		if !file.IsDir() {
			dat, err := ioutil.ReadFile(realPath)
			if err != nil {
				return err
			}
			// Add some files to the archive.
			f, err := w.Create(realPathInZip)
			if err != nil {
				return err
			}
			_, err = f.Write(dat)
			if err != nil {
				return err
			}
		} else if file.IsDir() {
			err := addFiles(w, realPath, realPathInZip)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ZipDirectory(p string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := zip.NewWriter(buf)
	err := addFiles(w, p, "")
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ZipFile(filename string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := zip.NewWriter(buf)
	_, name := filepath.Split(filename)
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	f, err := w.Create(name)
	if err != nil {
		return nil, err
	}
	_, err = f.Write(dat)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
