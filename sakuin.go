//go:generate  go run github.com/swaggo/swag/cmd/swag@latest init -g http/api.go

// Package sakuin
package sakuin

import (
	"context"
	"encoding/json"
	"io"

	pb "github.com/z5labs/sakuin/proto"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
)

type Config struct {
	ObjectStore   ObjectStore
	DocumentStore DocumentStore
	RandSrc       io.Reader
}

type Service struct {
	objDB ObjectStore
	docDB DocumentStore

	rander io.Reader
}

func New(cfg Config) *Service {
	return &Service{
		objDB:  cfg.ObjectStore,
		docDB:  cfg.DocumentStore,
		rander: cfg.RandSrc,
	}
}

func (s *Service) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*pb.GetObjectResponse, error) {
	obj, err := s.objDB.Get(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.GetObjectResponse{Content: obj}, nil
}

func (s *Service) UpdateObject(ctx context.Context, req *pb.UpdateObjectRequest) (*pb.UpdateObjectResponse, error) {
	return nil, s.objDB.Update(ctx, req.Id, req.Content)
}

func (s *Service) GetMetadata(ctx context.Context, req *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	metadata, err := s.docDB.Get(ctx, req.Id)
	if err != nil {
		zap.L().Error("unexpected error when getting metadata", zap.String("id", req.Id))
		return nil, err
	}

	any, err := marshalJSONToAny(metadata)
	if err != nil {
		return nil, err
	}

	return &pb.GetMetadataResponse{Metadata: any}, nil
}

func (s *Service) UpdateMetadata(ctx context.Context, req *pb.UpdateMetadataRequest) (*pb.UpdateMetadataResponse, error) {
	stats, err := s.docDB.Stat(ctx, req.Id)
	if err != nil {
		zap.L().Error("unexpected error when stat-ing metadata", zap.Error(err))
		return nil, err
	}
	if !stats.Exists {
		zap.L().Error("metadata doesn't exist", zap.String("id", req.Id))
		return nil, DocumentDoesNotExistErr{ID: req.Id}
	}

	metadata, err := unmarshalAnyToJSON(req.Metadata)
	if err != nil {
		return nil, err
	}

	zap.L().Info("updating metadata", zap.String("id", req.Id))
	return nil, s.docDB.Upsert(ctx, req.Id, metadata)
}

func (s *Service) Index(ctx context.Context, req *pb.IndexRequest) (*pb.IndexResponse, error) {
	id, err := s.generateUUID(ctx)
	if err != nil {
		return nil, err
	}

	g, gctx := errgroup.WithContext(ctx)

	// Upload object to object store
	g.Go(func() error {
		zap.L().Info("indexing object", zap.String("id", id))
		return s.objDB.Put(gctx, id, req.Object)
	})

	// Upload document to doc store
	if req.Metadata != nil {
		g.Go(func() error {
			metadata, err := unmarshalAnyToJSON(req.Metadata)
			if err != nil {
				return err
			}

			zap.L().Info("indexing metadata", zap.String("id", id))
			return s.docDB.Upsert(gctx, id, metadata)
		})
	}

	err = g.Wait()
	if err != nil {
		// TODO: cleanup
		return nil, err
	}

	return &pb.IndexResponse{Id: id}, nil
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

func marshalJSONToAny(m map[string]interface{}) (*anypb.Any, error) {
	b, err := json.Marshal(m)
	if err != nil {
		zap.L().Error("unexpected error when marshalling json", zap.Error(err))
		return nil, err
	}

	msg := pb.JSONMetadata{Json: b}
	return anypb.New(&msg)
}

func unmarshalAnyToJSON(any *anypb.Any) (map[string]interface{}, error) {
	var msg pb.JSONMetadata
	err := any.UnmarshalTo(&msg)
	if err != nil {
		zap.L().Error("unexpected error when unmarshalling any proto", zap.Error(err))
		return nil, err
	}

	var metadata map[string]interface{}
	err = json.Unmarshal(msg.Json, &metadata)
	if err != nil {
		zap.L().Error("unexpected error when unmarshalling json metadata", zap.Error(err))
		return nil, err
	}

	return metadata, nil
}
