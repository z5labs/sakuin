package http

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
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

		assert.Equal(subT, http.StatusNotFound, resp.StatusCode)
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

		if !assert.Equal(subT, http.StatusOK, resp.StatusCode) {
			return
		}

		obj, err := readAll(resp.Body)
		if err != nil {
			subT.Error(err)
			return
		}

		assert.Equal(subT, testObject, obj)
	})
}

func TestUpdateObjectHandler(t *testing.T) {
	t.Run("should fail if object doesn't exist", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		uri := fmt.Sprintf(getObjectEndpointFmt, addr, "objectDoesNotExistID")
		req, err := http.NewRequest(http.MethodPut, uri, bytes.NewReader([]byte("content")))
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		assert.Equal(subT, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("should succeed if object does exist", func(subT *testing.T) {
		testObjectID := "test"
		testObject := []byte("test object content")

		objStore := sakuin.NewInMemoryObjectStore().
			WithObject(testObjectID, testObject)

		addr, err := startTestServer(subT, withObjectStore(objStore))
		if err != nil {
			subT.Error(err)
			return
		}

		uri := fmt.Sprintf(getObjectEndpointFmt, addr, testObjectID)
		req, err := http.NewRequest(http.MethodPut, uri, bytes.NewReader([]byte("content")))
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		assert.Equal(subT, http.StatusOK, resp.StatusCode)
	})
}
