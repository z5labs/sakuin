package http

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/z5labs/sakuin"

	"github.com/stretchr/testify/assert"
)

const getMetadataEndpointFmt = "http://%s/index/%s/metadata"

func TestGetMetadataHandler(t *testing.T) {
	t.Run("should fail if metadata doesn't exist", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.Get(fmt.Sprintf(getMetadataEndpointFmt, addr, "metadataDoesNotExistID"))
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

	t.Run("should succeed if metadata exists", func(subT *testing.T) {
		testDocID := "test"
		testDoc := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}

		docStore := sakuin.NewInMemoryDocumentStore().
			WithDocument(testDocID, testDoc)

		addr, err := startTestServer(subT, withDocumentStore(docStore))
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.Get(fmt.Sprintf(getMetadataEndpointFmt, addr, testDocID))
		if err != nil {
			subT.Error(err)
			return
		}

		if resp.StatusCode != 200 {
			subT.Logf("unexpected http status code\n\texpected: %d\n\tactual: %d", 200, resp.StatusCode)
			subT.Fail()
			return
		}

		var doc map[string]interface{}
		if !decodeJSON(subT, resp.Body, &doc) {
			return
		}
		for k, v := range testDoc {
			dv, ok := doc[k]
			if !ok {
				subT.Logf("key not found\n\tkey: %s", k)
				subT.Fail()
				return
			}
			if dv != v {
				subT.Logf("values did not match\n\texpected: %s\n\tactual: %s", v, dv)
				subT.Fail()
				return
			}
		}
	})
}

func TestUpdateMetadataHandler(t *testing.T) {
	t.Run("should fail if req content type isn't json", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		uri := fmt.Sprintf(getMetadataEndpointFmt, addr, "metadataDoesNotExistID")
		req, err := http.NewRequest(http.MethodPut, uri, bytes.NewReader([]byte("{}")))
		if err != nil {
			subT.Error(err)
			return
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		assert.Equal(subT, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("should fail if metadata doesn't exist", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		uri := fmt.Sprintf(getMetadataEndpointFmt, addr, "metadataDoesNotExistID")
		req, err := http.NewRequest(http.MethodPut, uri, bytes.NewReader([]byte("{}")))
		if err != nil {
			subT.Error(err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		assert.Equal(subT, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("should succeed if metadata does exist", func(subT *testing.T) {
		testDocID := "test"
		testMetadata := map[string]interface{}{"hello": "world"}

		docStore := sakuin.NewInMemoryDocumentStore().
			WithDocument(testDocID, testMetadata)

		addr, err := startTestServer(subT, withDocumentStore(docStore))
		if err != nil {
			subT.Error(err)
			return
		}

		uri := fmt.Sprintf(getMetadataEndpointFmt, addr, testDocID)
		req, err := http.NewRequest(http.MethodPut, uri, bytes.NewReader([]byte(`{"good": "bye"}`)))
		if err != nil {
			subT.Error(err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		assert.Equal(subT, http.StatusOK, resp.StatusCode)
	})
}
