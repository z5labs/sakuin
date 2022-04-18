package sakuin

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"
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
		_, err := s.GetObject(context.Background(), &GetObjectRequest{
			ID: "",
		})

		if _, ok := err.(ObjectDoesNotExistErr); err == nil || !ok {
			subT.Log("expected error since object with given id doesn't exist")
			subT.Fail()
			return
		}
	})

	t.Run("should succeed if object exists", func(subT *testing.T) {
		resp, err := s.GetObject(context.Background(), &GetObjectRequest{
			ID: testObjectID,
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if !bytes.Equal(testObjectContent, resp.Object) {
			subT.Logf("expected object content to match\n\texpected: %s\n\tactual: %s", testObjectContent, resp.Object)
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

		_, err := s.UpdateObject(context.Background(), &UpdateObjectRequest{
			ID: "objectDoesNotExistID",
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

		resp, err := s.GetObject(context.Background(), &GetObjectRequest{
			ID: testObjectID,
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if !bytes.Equal(testObjectContent, resp.Object) {
			subT.Logf("expected object content to match\n\texpected: %s\n\tactual: %s", testObjectContent, resp.Object)
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
		_, err := s.GetMetadata(context.Background(), &GetMetadataRequest{
			ID: "",
		})

		if _, ok := err.(DocumentDoesNotExistErr); err == nil || !ok {
			subT.Log("expected error since document with given id doesn't exist")
			subT.Fail()
			return
		}
	})

	t.Run("should succeed if doc exists", func(subT *testing.T) {
		resp, err := s.GetMetadata(context.Background(), &GetMetadataRequest{
			ID: testGoodDocID,
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if testGoodDoc["name"] != resp.Metadata["name"] {
			subT.Logf("expected name to match\n\texpected: %s\n\tactual: %s", testGoodDoc["name"], resp.Metadata["name"])
			subT.Fail()
			return
		}

		if testGoodDoc["description"] != resp.Metadata["description"] {
			subT.Logf("expected description to match\n\texpected: %s\n\tactual: %s", testGoodDoc["description"], resp.Metadata["description"])
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

		resp, err := s.Index(context.Background(), &IndexRequest{
			Metadata: map[string]interface{}{
				"name":        "test",
				"description": "test",
			},
			Object: []byte("test object content"),
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if resp.ID == "" {
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

		resp, err := s.Index(context.Background(), &IndexRequest{
			Metadata: map[string]interface{}{
				"name":        "test",
				"description": "test",
			},
			Object: []byte("test object content"),
		})
		if err != nil {
			subT.Error(err)
			return
		}

		if resp.ID == "" {
			subT.Fail()
			return
		}
	})
}
