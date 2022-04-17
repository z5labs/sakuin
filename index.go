//go:generate  go run github.com/swaggo/swag/cmd/swag@latest init -g http/api.go

// Package sakuin
package sakuin

import (
	"context"
	"io"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	objDB ObjectStore
	docDB DocumentStore

	rander io.Reader
}

type Config struct {
	ObjectStore   ObjectStore
	DocumentStore DocumentStore
	RandSrc       io.Reader
}

func New(cfg Config) *Service {
	return &Service{
		objDB:  cfg.ObjectStore,
		docDB:  cfg.DocumentStore,
		rander: cfg.RandSrc,
	}
}

type GetObjectRequest struct {
	ID string
}

type GetObjectResponse struct {
	Object []byte
}

func (s *Service) GetObject(ctx context.Context, req *GetObjectRequest) (*GetObjectResponse, error) {
	obj, err := s.objDB.Get(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return &GetObjectResponse{Object: obj}, nil
}

type GetMetadataRequest struct {
	ID string
}

type GetMetadataResponse struct {
	Metadata map[string]interface{} `json:"metadata"`
}

func (s *Service) GetMetadata(ctx context.Context, req *GetMetadataRequest) (*GetMetadataResponse, error) {
	doc, err := s.docDB.Get(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	return &GetMetadataResponse{Metadata: doc}, nil
}

type IndexRequest struct {
	Metadata map[string]interface{}
	Object   []byte
}

type IndexResponse struct {
	ID string `json:"id"`
}

func (s *Service) Index(ctx context.Context, req *IndexRequest) (*IndexResponse, error) {
	id, err := s.generateUUID(ctx)
	if err != nil {
		return nil, err
	}

	g, gctx := errgroup.WithContext(ctx)

	// Upload object to object store
	g.Go(func() error {
		return s.objDB.Put(gctx, id, req.Object)
	})

	// Upload document to doc store
	g.Go(func() error {
		return s.docDB.Upsert(gctx, id, req.Metadata)
	})

	err = g.Wait()
	if err != nil {
		// TODO: cleanup
		return nil, err
	}

	return &IndexResponse{ID: id}, nil
}

func (s *Service) generateUUID(ctx context.Context) (string, error) {
	for {
		id := uuid.Must(uuid.NewRandomFromReader(s.rander)).String()
		stats, err := s.objDB.Stat(ctx, id)
		if err != nil {
			return "", err
		}
		if !stats.Exists {
			return id, nil
		}
	}
}
