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

// Config
type Config struct {
	ObjectStore   ObjectStore
	DocumentStore DocumentStore
	RandSrc       io.Reader
}

// Service
type Service struct {
	objDB ObjectStore
	docDB DocumentStore

	rander io.Reader
}

// New constructs a new service instance with a given configuration
func New(cfg Config) *Service {
	return &Service{
		objDB:  cfg.ObjectStore,
		docDB:  cfg.DocumentStore,
		rander: cfg.RandSrc,
	}
}

// Index
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

// GetFromIndex
func (s *Service) GetFromIndex(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	objCh := make(chan []byte, 1)
	metaCh := make(chan *anypb.Any, 1)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(objCh)

		obj, err := s.getObject(gctx, req)
		objCh <- obj

		return err
	})

	g.Go(func() error {
		defer close(metaCh)

		meta, err := s.getMetadata(gctx, req)
		metaCh <- meta

		return err
	})

	err := g.Wait()
	if err != nil {
		// TODO: cleanup
		zap.L().Error(
			"encountered error while retrieving object and metadata",
			zap.String("id", req.Id),
			zap.Error(err),
		)

		return nil, err
	}

	objPayload := &pb.Payload{
		Content: &pb.Payload_Object{
			Object: <-objCh,
		},
	}
	metaPayload := &pb.Payload{
		Content: &pb.Payload_Metadata{
			Metadata: <-metaCh,
		},
	}

	return &pb.GetResponse{Payloads: []*pb.Payload{objPayload, metaPayload}}, nil
}

func (s *Service) getMetadata(ctx context.Context, req *pb.GetRequest) (*anypb.Any, error) {
	metadata, err := s.docDB.Get(ctx, req.Id)
	if err != nil {
		zap.L().Error("unexpected error when getting metadata", zap.String("id", req.Id))
		return nil, err
	}

	return marshalJSONToAny(metadata)
}

func (s *Service) getObject(ctx context.Context, req *pb.GetRequest) ([]byte, error) {
	return s.objDB.Get(ctx, req.Id)
}

// UpdateIndex
func (s *Service) UpdateIndex(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	for _, payload := range req.Payloads {
		switch x := payload.Content.(type) {
		case *pb.Payload_Metadata:
			g.Go(func() error {
				err := s.updateMetadata(gctx, req.Id, x.Metadata)
				if err != nil {
					zap.L().Error(
						"unexpected error when deleting metadata",
						zap.String("id", req.Id),
						zap.Error(err),
					)
				}
				return err
			})
		case *pb.Payload_Object:
			g.Go(func() error {
				err := s.updateObject(gctx, req.Id, x.Object)
				if err != nil {
					zap.L().Error(
						"unexpected error when deleting object",
						zap.String("id", req.Id),
						zap.Error(err),
					)
				}
				return err
			})
		}
	}

	err := g.Wait()
	if err != nil {
		zap.L().Error(
			"unexpected error while waiting for object/metadata to be deleted",
			zap.String("id", req.Id),
			zap.Error(err),
		)

		return nil, err
	}

	return new(pb.UpdateResponse), nil
}

func (s *Service) updateMetadata(ctx context.Context, id string, metadata *anypb.Any) error {
	stats, err := s.docDB.Stat(ctx, id)
	if err != nil {
		zap.L().Error("unexpected error when stat-ing metadata", zap.String("id", id), zap.Error(err))
		return err
	}
	if !stats.Exists {
		zap.L().Error("metadata doesn't exist", zap.String("id", id))
		return DocumentDoesNotExistErr{ID: id}
	}

	jsonMeta, err := unmarshalAnyToJSON(metadata)
	if err != nil {
		return err
	}

	zap.L().Info("updating metadata", zap.String("id", id))
	return s.docDB.Upsert(ctx, id, jsonMeta)
}

func (s *Service) updateObject(ctx context.Context, id string, content []byte) error {
	return s.objDB.Update(ctx, id, content)
}

// DeleteFromIndex
func (s *Service) DeleteFromIndex(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return new(pb.DeleteResponse), nil
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
