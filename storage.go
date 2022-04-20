//go:generate go run github.com/vektra/mockery/v2@latest --name=ObjectStore --filename object_store_mock.go
//go:generate go run github.com/vektra/mockery/v2@latest --name=DocumentStore --filename document_store_mock.go

package sakuin

import (
	"context"
	"sync"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type ObjectDoesNotExistErr struct {
	ID string
}

func (e ObjectDoesNotExistErr) Error() string {
	return e.ID
}

type DocumentDoesNotExistErr struct {
	ID string
}

func (e DocumentDoesNotExistErr) Error() string {
	return e.ID
}

type StatInfo struct {
	Exists bool
	Size   int
}

// ObjectStore represents an Object database.
type ObjectStore interface {
	// Stat retrieve info about object by its id.
	Stat(ctx context.Context, id string) (*StatInfo, error)

	// Get object by id.
	Get(ctx context.Context, id string) ([]byte, error)

	// Put a new object into storage with the given id.
	Put(ctx context.Context, id string, b []byte) error

	// Update object by id.
	Update(ctx context.Context, id string, b []byte) error

	// Delete object by id.
	Delete(ctx context.Context, id string) error
}

type TestingT interface {
	assert.TestingT
	Run(name string, f func(TestingT))
}

// RunObjectStorageTests runs a suite of functional tests which can be
// used to verify if a particular ObjectStore follows the expected
// behaviour.
//
func RunObjectStorageTests(t TestingT, objStore ObjectStore) {
	t.Run("get object should fail with ObjectDoesNotExistErr if object doesn't exist", func(subT TestingT) {
		var objErr ObjectDoesNotExistErr
		_, err := objStore.Get(context.Background(), "")
		assert.ErrorAs(subT, err, &objErr, "expected an ObjectDoesNotExistErr")
	})

	t.Run("update object should fail with ObjectDoesNotExistErr if object doesn't exist", func(subT TestingT) {
		var objErr ObjectDoesNotExistErr
		err := objStore.Update(context.Background(), "", []byte{})
		assert.ErrorAs(subT, err, &objErr, "expected an ObjectDoesNotExistErr")
	})

	t.Run("delete object should fail with ObjectDoesNotExistErr if object doesn't exist", func(subT TestingT) {
		var objErr ObjectDoesNotExistErr
		err := objStore.Delete(context.Background(), "")
		assert.ErrorAs(subT, err, &objErr, "expected an ObjectDoesNotExistErr")
	})
}

// InMemoryObjectStore implements the ObjectStore by storing all objects in memory.
// This SHOULD only be used for TESTING only.
//
type InMemoryObjectStore struct {
	objects sync.Map
}

func NewInMemoryObjectStore() *InMemoryObjectStore {
	return &InMemoryObjectStore{}
}

func (s *InMemoryObjectStore) Stat(ctx context.Context, id string) (*StatInfo, error) {
	var numOfObjects int
	obj, exists := s.objects.Load(id)
	if obj != nil {
		numOfObjects = len(obj.([]byte))
	}

	return &StatInfo{Exists: exists, Size: numOfObjects}, nil
}

func (s *InMemoryObjectStore) Get(ctx context.Context, id string) ([]byte, error) {
	obj, exists := s.objects.Load(id)
	if !exists {
		zap.L().Warn("unable to find object in memory", zap.String("id", id))
		return nil, ObjectDoesNotExistErr{ID: id}
	}
	zap.L().Debug("successfully retrieved object from memory", zap.String("id", id))

	return obj.([]byte), nil
}

func (s *InMemoryObjectStore) Put(ctx context.Context, id string, b []byte) error {
	s.objects.Store(id, b)
	zap.L().Debug("successfully stored object in memory", zap.String("id", id))

	return nil
}

func (s *InMemoryObjectStore) Update(ctx context.Context, id string, b []byte) error {
	if _, exists := s.objects.Load(id); !exists {
		return ObjectDoesNotExistErr{ID: id}
	}
	s.objects.Store(id, b)

	zap.L().Debug("successfully updated object in memory", zap.String("id", id))
	return nil
}

func (s *InMemoryObjectStore) Delete(ctx context.Context, id string) error {
	if _, exists := s.objects.Load(id); !exists {
		return ObjectDoesNotExistErr{ID: id}
	}
	s.objects.Delete(id)

	zap.L().Debug("successfully deleted object", zap.String("id", id))
	return nil
}

func (s *InMemoryObjectStore) WithObject(id string, obj []byte) *InMemoryObjectStore {
	s.objects.Store(id, obj)
	return s
}

func (s *InMemoryObjectStore) NumOfObects() int {
	var n int
	s.objects.Range(func(key, value any) bool {
		n += 1
		return true
	})
	return n
}

// DocumentStore represents a Document database.
type DocumentStore interface {
	// Stat retrieve document info by id.
	Stat(ctx context.Context, id string) (*StatInfo, error)

	// Get document by id.
	Get(ctx context.Context, id string) (map[string]interface{}, error)

	// Upsert document.
	Upsert(ctx context.Context, id string, b map[string]interface{}) error

	// Delete document by id
	Delete(ctx context.Context, id string) error
}

// RunDocumentStorageTests runs a suite of functional tests which can be
// used to verify if a particular DocumentStore follows the expected
// behaviour.
//
func RunDocumentStorageTests(t TestingT, docStore DocumentStore) {
	t.Run("get document should fail with DocumentDoesNotExistErr if document doesn't exist", func(subT TestingT) {
		var docErr DocumentDoesNotExistErr
		_, err := docStore.Get(context.Background(), "")
		assert.ErrorAs(subT, err, &docErr, "expected and DocumentDoesNotExistErr")
	})

	t.Run("delete document should fail with DocumentDoesNotExistErr if document doesn't exist", func(subT TestingT) {
		var docErr DocumentDoesNotExistErr
		err := docStore.Delete(context.Background(), "")
		assert.ErrorAs(subT, err, &docErr, "expected and DocumentDoesNotExistErr")
	})
}

// InMemoryDocumentStore implements the DocumentStore by storing all documents in memory.
// This SHOULD only be used for TESTING only.
//
type InMemoryDocumentStore struct {
	docs sync.Map
}

func NewInMemoryDocumentStore() *InMemoryDocumentStore {
	return &InMemoryDocumentStore{}
}

func (s *InMemoryDocumentStore) Stat(ctx context.Context, id string) (*StatInfo, error) {
	var numOfFields int
	doc, exists := s.docs.Load(id)
	if doc != nil {
		numOfFields = len(doc.(map[string]interface{}))
	}

	return &StatInfo{Exists: exists, Size: numOfFields}, nil
}

func (s *InMemoryDocumentStore) Get(ctx context.Context, id string) (map[string]interface{}, error) {
	doc, exists := s.docs.Load(id)
	if !exists {
		zap.L().Warn("unable to retrieve document from memory", zap.String("id", id))
		return nil, DocumentDoesNotExistErr{ID: id}
	}
	zap.L().Debug("successfully retrieved document from memory", zap.String("id", id))

	return doc.(map[string]interface{}), nil
}

func (s *InMemoryDocumentStore) Upsert(ctx context.Context, id string, doc map[string]interface{}) error {
	d, ok := s.docs.Load(id)
	if ok {
		doc = mergeDocs(doc, d.(map[string]interface{}))
	}
	s.docs.Store(id, doc)
	zap.L().Debug("successfully stored document in memory", zap.String("id", id))

	return nil
}

func (s *InMemoryDocumentStore) Delete(ctx context.Context, id string) error {
	if _, exists := s.docs.Load(id); !exists {
		return DocumentDoesNotExistErr{ID: id}
	}
	s.docs.Delete(id)
	return nil
}

func (s *InMemoryDocumentStore) WithDocument(id string, doc map[string]interface{}) *InMemoryDocumentStore {
	s.docs.Store(id, doc)
	return s
}

func (s *InMemoryDocumentStore) NumOfDocs() int {
	var n int
	s.docs.Range(func(key, val any) bool {
		n += 1
		return true
	})
	return n
}

func mergeDocs(dst, src map[string]interface{}) map[string]interface{} {
	for k, sv := range src {
		dv, exists := dst[k]
		if !exists {
			dst[k] = sv
			continue
		}

		svMap, ok := sv.(map[string]interface{})
		if !ok {
			continue
		}

		dvMap, ok := dv.(map[string]interface{})
		if !ok {
			panic("expected documents to have consistent type for given field")
		}

		mergeDocs(dvMap, svMap)
	}

	return dst
}
