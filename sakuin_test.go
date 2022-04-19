package sakuin

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"

	pb "github.com/z5labs/sakuin/proto"
)

func TestGetObject(t *testing.T) {
	objStore := NewInMemoryObjectStore()

	testObjectID := "testObject"
	testObjectContent := []byte("test content")
	err := objStore.Put(context.Background(), testObjectID, testObjectContent)
	if err != nil {
		t.Error(err)
		return
	}

	s := New(Config{
		ObjectStore: objStore,
	})

	t.Run("should fail if ID doesn't exist", func(subT *testing.T) {
		_, err := s.GetObject(context.Background(), &pb.GetObjectRequest{
			Id: "",
		})

		if _, ok := err.(ObjectDoesNotExistErr); err == nil || !ok {
			subT.Log("expected error since object with given id doesn't exist")
			subT.Fail()
			return
		}
	})

	t.Run("should succeed if object exists", func(subT *testing.T) {
		resp, err := s.GetObject(context.Background(), &pb.GetObjectRequest{
			Id: testObjectID,
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if !bytes.Equal(testObjectContent, resp.Content) {
			subT.Logf("expected object content to match\n\texpected: %s\n\tactual: %s", testObjectContent, resp.Content)
			subT.Fail()
			return
		}
	})
}

func TestUpdateObject(t *testing.T) {
	t.Run("should fail if ID doesn't exist", func(subT *testing.T) {
		s := New(Config{
			ObjectStore: NewInMemoryObjectStore(),
		})

		_, err := s.UpdateObject(context.Background(), &pb.UpdateObjectRequest{
			Id: "objectDoesNotExistID",
		})

		if _, ok := err.(ObjectDoesNotExistErr); err == nil || !ok {
			subT.Log("expected error since object with given id doesn't exist")
			subT.Fail()
			return
		}
	})

	t.Run("should succeed if object exists", func(subT *testing.T) {
		objStore := NewInMemoryObjectStore()

		testObjectID := "testObject"
		testObjectContent := []byte("test content")
		err := objStore.Put(context.Background(), testObjectID, testObjectContent)
		if err != nil {
			t.Error(err)
			return
		}

		s := New(Config{
			ObjectStore: objStore,
		})

		resp, err := s.GetObject(context.Background(), &pb.GetObjectRequest{
			Id: testObjectID,
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if !bytes.Equal(testObjectContent, resp.Content) {
			subT.Logf("expected object content to match\n\texpected: %s\n\tactual: %s", testObjectContent, resp.Content)
			subT.Fail()
			return
		}
	})
}

func TestGetMetadata(t *testing.T) {
	docStore := NewInMemoryDocumentStore()

	testGoodDocID := "testGoodDoc"
	testGoodDoc := map[string]interface{}{
		"name":        "test",
		"description": "test description",
	}
	err := docStore.Upsert(context.Background(), testGoodDocID, testGoodDoc)
	if err != nil {
		t.Error(err)
		return
	}

	s := New(Config{
		DocumentStore: docStore,
	})

	t.Run("should fail if ID doesn't exist", func(subT *testing.T) {
		_, err := s.GetMetadata(context.Background(), &pb.GetMetadataRequest{
			Id: "",
		})

		if _, ok := err.(DocumentDoesNotExistErr); err == nil || !ok {
			subT.Log("expected error since document with given id doesn't exist")
			subT.Fail()
			return
		}
	})

	t.Run("should succeed if doc exists", func(subT *testing.T) {
		resp, err := s.GetMetadata(context.Background(), &pb.GetMetadataRequest{
			Id: testGoodDocID,
		})
		if err != nil {
			subT.Error(err)
			return
		}

		metadata, err := unmarshalAnyToJSON(resp.Metadata)
		if err != nil {
			subT.Error(err)
			return
		}

		if testGoodDoc["name"] != metadata["name"] {
			subT.Logf("expected name to match\n\texpected: %s\n\tactual: %s", testGoodDoc["name"], metadata["name"])
			subT.Fail()
			return
		}

		if testGoodDoc["description"] != metadata["description"] {
			subT.Logf("expected description to match\n\texpected: %s\n\tactual: %s", testGoodDoc["description"], metadata["description"])
			subT.Fail()
			return
		}
	})
}

func TestIndex(t *testing.T) {
	objStore := NewInMemoryObjectStore()
	docStore := NewInMemoryDocumentStore()

	t.Run("should succeed", func(subT *testing.T) {
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
