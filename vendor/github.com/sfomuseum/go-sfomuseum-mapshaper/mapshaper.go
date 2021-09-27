package mapshaper

import (
	"context"
	"errors"
	"os"
	"os/exec"
)

type Mapshaper struct {
	path string
}

func NewMapshaper(ctx context.Context, path string) (*Mapshaper, error) {

	info, err := os.Stat(path)

	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return nil, errors.New("Invalid path")
	}

	ms := &Mapshaper{
		path: path,
	}

	return ms, nil
}

func (ms *Mapshaper) Call(ctx context.Context, args ...string) ([]byte, error) {

	cmd := exec.CommandContext(ctx, ms.path, args...)
	return cmd.Output()
}
