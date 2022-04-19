package sakuin

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"mime/multipart"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadParts(t *testing.T) {
	t.Run("should fail if content type isn't multipart/form-data", func(subT *testing.T) {
		_, _, err := ReadParts(nil, "application/json")
		assert.ErrorIs(subT, err, ContentTypeError{ContentType: "application/json"})
	})

	t.Run("should fail if missing boundary", func(subT *testing.T) {
		_, _, err := ReadParts(nil, "multipart/form-data")
		assert.ErrorIs(subT, ErrMissingBoundary, err)
	})

	t.Run("should succeed if metadata part is before object part", func(subT *testing.T) {
		testMetadata := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}
		testObject := []byte("test object content")

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		mw, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="metadata"`},
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
		})
		if err != nil {
			subT.Error(err)
			return
		}
		ow.Write(testObject)

		w.Close()

		metadata, obj, err := ReadParts(&b, w.FormDataContentType())
		if err != nil {
			subT.Error(err)
			return
		}

		var m map[string]interface{}
		err = json.Unmarshal(metadata, &m)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, testMetadata, m) {
			return
		}

		if !assert.Equal(subT, testObject, obj) {
			return
		}
	})

	t.Run("should succeed if object part is before metadata part", func(subT *testing.T) {
		testMetadata := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}
		testObject := []byte("test object content")

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		ow, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="object"`},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		ow.Write(testObject)

		mw, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="metadata"`},
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

		metadata, obj, err := ReadParts(&b, w.FormDataContentType())
		if err != nil {
			subT.Error(err)
			return
		}

		var m map[string]interface{}
		err = json.Unmarshal(metadata, &m)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, testMetadata, m) {
			return
		}

		if !assert.Equal(subT, testObject, obj) {
			return
		}
	})

	t.Run("should succeed if missing metadata part", func(subT *testing.T) {
		testObject := []byte("test object content")

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		ow, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="object"`},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		ow.Write(testObject)

		w.Close()

		metadata, obj, err := ReadParts(&b, w.FormDataContentType())
		if err != nil {
			subT.Error(err)
			return
		}

		if metadata != nil {
			subT.Log("expected metadata to be nil")
			subT.Fail()
			return
		}

		if !assert.Equal(subT, testObject, obj) {
			return
		}
	})

	t.Run("should succeed if missing object part", func(subT *testing.T) {
		testMetadata := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		mw, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="metadata"`},
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

		metadata, obj, err := ReadParts(&b, w.FormDataContentType())
		if err != nil {
			subT.Error(err)
			return
		}

		if obj != nil {
			subT.Log("expected obj to be nil")
			subT.Fail()
			return
		}

		var m map[string]interface{}
		err = json.Unmarshal(metadata, &m)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, testMetadata, m) {
			return
		}
	})

	t.Run("object content type doesn't have to be octet-stream", func(subT *testing.T) {
		testObject := map[string]interface{}{
			"name":        "test",
			"description": "test description",
		}

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		ow, err := w.CreatePart(map[string][]string{
			"Content-Disposition": {`form-data; name="object"`},
		})
		if err != nil {
			subT.Error(err)
			return
		}
		enc := json.NewEncoder(ow)
		if err = enc.Encode(testObject); err != nil {
			subT.Error(err)
			return
		}

		w.Close()

		metadata, obj, err := ReadParts(&b, w.FormDataContentType())
		if err != nil {
			subT.Error(err)
			return
		}

		if metadata != nil {
			subT.Log("expected metadata to be nil")
			subT.Fail()
			return
		}

		var m map[string]interface{}
		err = json.Unmarshal(obj, &m)
		if err != nil {
			subT.Error(err)
			return
		}

		if !assert.Equal(subT, testObject, m) {
			return
		}
	})
}

// BenchmarkReadParts
// metadata has 3 fields
// object size is 10MB
//
func BenchmarkReadParts(b *testing.B) {
	testMetadata := map[string]interface{}{
		"name":  "test",
		"id":    "test",
		"email": "test",
	}

	testObject := make([]byte, 10000000)
	_, err := rand.Read(testObject)
	if err != nil {
		b.Error(err)
		return
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	mw, err := w.CreatePart(map[string][]string{
		"Content-Disposition": {`form-data; name="metadata"`},
		"Content-Type":        {"application/json"},
	})
	if err != nil {
		b.Error(err)
		return
	}
	enc := json.NewEncoder(mw)
	if err = enc.Encode(testMetadata); err != nil {
		b.Error(err)
		return
	}

	ow, err := w.CreatePart(map[string][]string{
		"Content-Disposition": {`form-data; name="object"`},
		"Content-Type":        {"application/octet-stream"},
	})
	if err != nil {
		b.Error(err)
		return
	}
	ow.Write(testObject)

	w.Close()

	r := bytes.NewReader(buf.Bytes())
	contentType := w.FormDataContentType()

	for i := 0; i < b.N; i++ {
		_, _, err := ReadParts(r, contentType)
		if err != nil {
			b.Error(err)
			return
		}
		r.Seek(0, io.SeekStart)
	}
}
