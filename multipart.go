package sakuin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"strings"

	"go.uber.org/zap"
)

// ErrMissingBoundary represents a boundary value missing in a multipart form body.
var ErrMissingBoundary = errors.New("missing boundary")

type ContentTypeError struct {
	ContentType string
}

func (e ContentTypeError) Error() string {
	return fmt.Sprintf("invalid content type: %s", e.ContentType)
}

// ReadParts
func ReadParts(r io.Reader, contentType string) (metadata json.RawMessage, object []byte, err error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		zap.L().Error("", zap.Error(err))
		return nil, nil, err
	}
	if !strings.HasPrefix(mediaType, "multipart/form-data") {
		zap.L().Error("unexpected media type", zap.String("content-type", mediaType))
		return nil, nil, ContentTypeError{ContentType: mediaType}
	}
	zap.L().Debug("parsed media type", zap.String("media-type", mediaType), zap.Any("params", params))

	boundary, ok := params["boundary"]
	if !ok {
		zap.L().Error("missing boundary")
		return nil, nil, ErrMissingBoundary
	}

	var p *multipart.Part
	mr := multipart.NewReader(r, boundary)
	for {
		p, err = mr.NextPart()
		if err == io.EOF {
			err = nil
			return
		}
		if err != nil {
			zap.L().Error("unexpected error when getting next part", zap.Error(err))
			return
		}

		pName := p.FormName()
		zap.L().Debug("read part", zap.String("name", pName))
		switch pName {
		case "metadata":
			dec := json.NewDecoder(p)
			err = dec.Decode(&metadata)
			if err != nil {
				zap.L().Error("unexpected error when decoding metadata part", zap.Error(err))
				return
			}
		case "object":
			object, err = ioutil.ReadAll(p)
			if err != nil {
				zap.L().Error("unexpected error when reading object content", zap.Error(err))
				return
			}
		}
	}
}
