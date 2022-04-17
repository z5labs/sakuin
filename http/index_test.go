package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"testing"

	"github.com/z5labs/sakuin/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sakuinEndpointFmt = "http://%s/index"

func TestIndexHandler(t *testing.T) {
	t.Run("should succeed if metadata and object are present", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		testMetadata := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}
		testObject := []byte("test object content")

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		mw, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="metadata"`},
			"Content-Type":        {"application/json"},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		enc := json.NewEncoder(mw)
		if err = enc.Encode(testMetadata); err != nil {
			subT.Error(err)
			return
		}

		ow, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="object"`},
			"Content-Type":        {"application/octet-stream"},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		ow.Write(testObject)

		w.Close()

		req, err := http.NewRequest("POST", fmt.Sprintf(sakuinEndpointFmt, addr), &b)
		if err != nil {
			subT.Error(err)
			return
		}
		req.Header.Set("Content-Type", w.FormDataContentType())

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, 200, resp.StatusCode) {
			return
		}

		var data map[string]interface{}
		if !decodeJSON(subT, resp.Body, &data) {
			return
		}

		assert.NotZero(subT, data["id"])
	})

	t.Run("should not fail if missing metadata part", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		testObject := []byte("test object content")

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		ow, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="object"`},
			"Content-Type":        {"application/octet-stream"},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		ow.Write(testObject)

		w.Close()

		req, err := http.NewRequest("POST", fmt.Sprintf(sakuinEndpointFmt, addr), &b)
		if err != nil {
			subT.Error(err)
			return
		}
		req.Header.Set("Content-Type", w.FormDataContentType())

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, 200, resp.StatusCode) {
			return
		}

		var data map[string]interface{}
		if !decodeJSON(subT, resp.Body, &data) {
			return
		}

		assert.NotZero(subT, data["id"])
	})

	t.Run("should fail if missing object part", func(subT *testing.T) {
		addr, err := startTestServer(subT)
		if err != nil {
			subT.Error(err)
			return
		}

		testMetadata := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		mw, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="metadata"`},
			"Content-Type":        {"application/json"},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		enc := json.NewEncoder(mw)
		if err = enc.Encode(testMetadata); err != nil {
			subT.Error(err)
			return
		}

		w.Close()

		req, err := http.NewRequest("POST", fmt.Sprintf(sakuinEndpointFmt, addr), &b)
		if err != nil {
			subT.Error(err)
			return
		}
		req.Header.Set("Content-Type", w.FormDataContentType())

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, 400, resp.StatusCode) {
			return
		}

		var apiErr APIError
		if !decodeJSON(subT, resp.Body, &apiErr) {
			return
		}

		assert.Equal(subT, ErrMissingObjectPart, apiErr)
	})

	t.Run("should undo storage actions if one fails", func(subT *testing.T) {
		mockDocStore := mocks.DocumentStore{}
		mockDocStore.On("Upsert", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("oh no something went wrong"))

		addr, err := startTestServer(subT, withDocumentStore(&mockDocStore))
		if err != nil {
			subT.Error(err)
			return
		}

		testMetadata := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}
		testObject := []byte("test object content")

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		mw, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="metadata"`},
			"Content-Type":        {"application/json"},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		enc := json.NewEncoder(mw)
		if err = enc.Encode(testMetadata); err != nil {
			subT.Error(err)
			return
		}

		ow, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="object"`},
			"Content-Type":        {"application/octet-stream"},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		ow.Write(testObject)

		w.Close()

		req, err := http.NewRequest("POST", fmt.Sprintf(sakuinEndpointFmt, addr), &b)
		if err != nil {
			subT.Error(err)
			return
		}
		req.Header.Set("Content-Type", w.FormDataContentType())

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, 500, resp.StatusCode) {
			return
		}
	})
}
