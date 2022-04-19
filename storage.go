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

type ObjectStore interface {
	Stat(ctx context.Context, id string) (*StatInfo, error)
	Get(ctx context.Context, id string) ([]byte, error)
	Put(ctx context.Context, id string, b []byte) error
	Update(ctx context.Context, id string, b []byte) error
}

type TestingT interface {
	assert.TestingT
	Run(name string, f func(TestingT))
}

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
}

type InMemoryObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func NewInMemoryObjectStore() *InMemoryObjectStore {
	return &InMemoryObjectStore{
		objects: make(map[string][]byte),
	}
}

func (s *InMemoryObjectStore) Stat(ctx context.Context, id string) (*StatInfo, error) {
	s.mu.Lock()
	obj, exists := s.objects[id]
	s.mu.Unlock()

	return &StatInfo{Exists: exists, Size: len(obj)}, nil
}

func (s *InMemoryObjectStore) Get(ctx context.Context, id string) ([]byte, error) {
	s.mu.Lock()
	obj, exists := s.objects[id]
	s.mu.Unlock()
	if !exists {
		zap.L().Warn("unable to find object in memory", zap.String("id", id))
		return nil, ObjectDoesNotExistErr{ID: id}
	}
	zap.L().Debug("successfully retrieved object from memory", zap.String("id", id))

	return obj, nil
}

func (s *InMemoryObjectStore) Put(ctx context.Context, id string, b []byte) error {
	s.mu.Lock()
	s.objects[id] = b
	s.mu.Unlock()
	zap.L().Debug("successfully stored object in memory", zap.String("id", id))

	return nil
}

func (s *InMemoryObjectStore) Update(ctx context.Context, id string, b []byte) error {
	s.mu.Lock()
	if _, exists := s.objects[id]; !exists {
		return ObjectDoesNotExistErr{ID: id}
	}
	s.objects[id] = b
	s.mu.Unlock()

	zap.L().Debug("successfully updated object in memory", zap.String("id", id))
	return nil
}

func (s *InMemoryObjectStore) WithObject(id string, obj []byte) *InMemoryObjectStore {
	s.objects[id] = obj
	return s
}

func (s *InMemoryObjectStore) NumOfObects() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.objects)
}

type DocumentStore interface {
	Stat(ctx context.Context, id string) (*StatInfo, error)
	Get(ctx context.Context, id string) (map[string]interface{}, error)
	Upsert(ctx context.Context, id string, b map[string]interface{}) error
}

func RunDocumentStorageTests(t TestingT, docStore DocumentStore) {
	t.Run("should fail with DocumentDoesNotExistErr if document doesn't exist", func(subT TestingT) {
		var docErr DocumentDoesNotExistErr
		_, err := docStore.Get(context.Background(), "")
		assert.ErrorAs(subT, err, &docErr, "expected and DocumentDoesNotExistErr")
	})
}

type InMemoryDocumentStore struct {
	mu   sync.Mutex
	docs map[string]map[string]interface{}
}

func NewInMemoryDocumentStore() *InMemoryDocumentStore {
	return &InMemoryDocumentStore{
		docs: make(map[string]map[string]interface{}),
	}
}

func (s *InMemoryDocumentStore) Stat(ctx context.Context, id string) (*StatInfo, error) {
	s.mu.Lock()
	doc, exists := s.docs[id]
	s.mu.Unlock()

	return &StatInfo{Exists: exists, Size: len(doc)}, nil
}

func (s *InMemoryDocumentStore) Get(ctx context.Context, id string) (map[string]interface{}, error) {
	s.mu.Lock()
	doc, exists := s.docs[id]
	s.mu.Unlock()
	if !exists {
		zap.L().Warn("unable to retrieve document from memory", zap.String("id", id))
		return nil, DocumentDoesNotExistErr{ID: id}
	}
	zap.L().Debug("successfully retrieved document from memory", zap.String("id", id))

	return doc, nil
}

func (s *InMemoryDocumentStore) Upsert(ctx context.Context, id string, doc map[string]interface{}) error {
	s.mu.Lock()
	d, ok := s.docs[id]
	if ok {
		doc = mergeDocs(doc, d)
	}
	s.docs[id] = doc
	s.mu.Unlock()
	zap.L().Debug("successfully stored document in memory", zap.String("id", id))

	return nil
}

func (s *InMemoryDocumentStore) WithDocument(id string, doc map[string]interface{}) *InMemoryDocumentStore {
	s.docs[id] = doc
	return s
}

func (s *InMemoryDocumentStore) NumOfDocs() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.docs)
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
