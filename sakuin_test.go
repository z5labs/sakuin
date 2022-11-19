package sakuin

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"

	pb "github.com/z5labs/sakuin/proto"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	t.Run("should succeed", func(subT *testing.T) {
		objStore := NewInMemoryObjectStore()
		docStore := NewInMemoryDocumentStore()

		s := New(Config{
			ObjectStore:   objStore,
			DocumentStore: docStore,
			RandSrc:       rand.Reader,
		})

		metadata, err := marshalJSONToAny(map[string]interface{}{
			"name":        "test",
			"description": "test",
		})
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := s.Index(context.Background(), &pb.IndexRequest{
			Metadata: metadata,
			Object:   []byte("test object content"),
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if resp.Id == "" {
			subT.Fail()
			return
		}

		if objStore.NumOfObects() != 1 {
			subT.Fail()
			return
		}

		if docStore.NumOfDocs() != 1 {
			subT.Fail()
			return
		}
	})

	t.Run("should succeed even if uuid already exists in db", func(subT *testing.T) {
		objStore := NewInMemoryObjectStore()
		docStore := NewInMemoryDocumentStore()

		same := "0123456789ABCDEF"
		different := "FEDBCA9876543210"

		s := New(Config{
			ObjectStore:   objStore,
			DocumentStore: docStore,
			RandSrc:       strings.NewReader(same + same + different),
		})

		metadata, err := marshalJSONToAny(map[string]interface{}{
			"name":        "test",
			"description": "test",
		})
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := s.Index(context.Background(), &pb.IndexRequest{
			Metadata: metadata,
			Object:   []byte("test object content"),
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if resp.Id == "" {
			subT.Fail()
			return
		}
	})
}

func TestGetFromIndex(t *testing.T) {
	t.Run("should succeed if object and metadata are present", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should succeed if only object is present", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should succeed if only metadata is present", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should fail if nothing exists", func(subT *testing.T) {
		subT.Fail()
	})
}

func TestUpdateIndex(t *testing.T) {
	t.Run("should succeed when updating both", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should succeed when updating just object content", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should succeed when updating just metadata", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should returns error if updating both fails", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should returns error if just updating object fails", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should returns error if just updating metadata fails", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should return error if id doesn't exist", func(subT *testing.T) {
		subT.Fail()
	})
}

func TestDeleteFromIndex(t *testing.T) {
	t.Run("should succeed if id exists", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should return error if just deleting object fails", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should return error if just deleting metadata fails", func(subT *testing.T) {
		subT.Fail()
	})

	t.Run("should return error if id doesn't exist", func(subT *testing.T) {
		s := New(Config{
			ObjectStore: NewInMemoryObjectStore(),
			DocumentStore: NewInMemoryDocumentStore(),
		})

		req := &pb.DeleteRequest{
			Id: "testId",
		}

		_, err := s.DeleteFromIndex(context.Background(), req)

		assert.Equal(subT, nil, err)
	})
}
