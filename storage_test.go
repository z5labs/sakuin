package sakuin

import (
	"testing"
)

type testingT struct {
	*testing.T
}

func liftTestingT(t *testing.T) TestingT {
	return testingT{t}
}

func (t testingT) Run(name string, f func(TestingT)) {
	t.T.Run(name, func(subT *testing.T) {
		f(liftTestingT(subT))
	})
}

func TestInMemoryObjectStore(t *testing.T) {
	RunObjectStorageTests(liftTestingT(t), NewInMemoryObjectStore())
}

func TestInMemoryDocumentStore(t *testing.T) {
	RunDocumentStorageTests(liftTestingT(t), NewInMemoryDocumentStore())
}
