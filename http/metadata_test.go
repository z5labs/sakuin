package http

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/z5labs/sakuin"
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
