package http

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/z5labs/sakuin"
)

const getObjectEndpointFmt = "http://%s/index/%s/object"

func TestGetObjectHandler(t *testing.T) {
	t.Run("should fail if object doesn't exist", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.Get(fmt.Sprintf(getObjectEndpointFmt, addr, "objectDoesNotExistID"))
		if err != nil {
			subT.Error(err)
			return
		}
		if resp.StatusCode != 404 {
			subT.Logf("unexpected http status code\n\texpected: %d\n\tactual: %d", 404, resp.StatusCode)
			subT.Fail()
			return
		}
	})

	t.Run("should succeed if object exists", func(subT *testing.T) {
		testObjectID := "test"
		testObject := []byte("test object content")

		objStore := sakuin.NewInMemoryObjectStore().
			WithObject(testObjectID, testObject)

		addr, err := startTestServer(subT, withObjectStore(objStore))
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.Get(fmt.Sprintf(getObjectEndpointFmt, addr, testObjectID))
		if err != nil {
			subT.Error(err)
			return
		}

		if resp.StatusCode != 200 {
			subT.Logf("unexpected http status code\n\texpected: %d\n\tactual: %d", 200, resp.StatusCode)
			subT.Fail()
			return
		}

		obj, err := readAll(resp.Body)
		if err != nil {
			subT.Error(err)
			return
		}
		if !bytes.Equal(testObject, obj) {
			subT.Log("object response doesn't match")
			subT.Fail()
			return
		}
	})
}
