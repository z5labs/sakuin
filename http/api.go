// Package http
package http

import (
	"bytes"
	"fmt"

	"github.com/z5labs/sakuin"
	"github.com/z5labs/sakuin/http/middleware/logger"

	swagger "github.com/arsmn/fiber-swagger/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"go.uber.org/zap"
)

// APIError
type APIError struct {
	Message string `json:"message"`
}

func (e APIError) Error() string {
	return fmt.Sprintf("api error: %s", e.Message)
}

var (
	ErrMissingObjectPart = APIError{
		Message: "must provide object part in form data",
	}
)

// @title           Sakuin RESTful API
// @version         0.0
// @description     Sakuin is a REST based service for indexing objects along with metadata.
// @termsOfService  http://swagger.io/terms/

// @contact.name   Z5Labs
// @contact.url    http://www.swagger.io/support
// @contact.email  support@swagger.io

// @license.name  MIT
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html

// @BasePath  /
// @schemes   http https

func NewServer(s *sakuin.Service, cfg ...fiber.Config) *fiber.App {
	app := fiber.New(cfg...)

	// Swagger
	app.Get("/swagger/*", swagger.HandlerDefault)

	// Object
	app.Get("/index/:id/object", NewGetObjectHandler(s))
	app.Put("/index/:id/object", NewUpdateObjectHandler(s))

	// Metadata
	app.Get("/index/:id/metadata", NewGetMetadataHandler(s))

	// Indexing
	app.Post("/index", NewIndexHandler(s))

	app.Use(
		pprof.New(),
		logger.New(),
		compress.New(compress.Config{
			Level: compress.LevelBestSpeed,
		}),
	)

	return app
}

// NewGetObjectHandler godoc
// @Summary  Retrieve an object.
// @Tags     Objects
// @Accept   json
// @Produce  application/zip
// @Param    id  path  string  true  "Object ID"
// @Router   /index/{id}/object [get]
func NewGetObjectHandler(s *sakuin.Service) fiber.Handler {
	return func(c *fiber.Ctx) error {
		c.AcceptsEncodings("gzip", "compress", "br")
		id := c.Params("id")

		resp, err := s.GetObject(c.Context(), &sakuin.GetObjectRequest{
			ID: id,
		})
		if _, ok := err.(sakuin.ObjectDoesNotExistErr); ok {
			zap.L().Error("object does not exist", zap.String("id", id))
			return c.SendStatus(fiber.StatusNotFound)
		}
		if err != nil {
			zap.L().Error("unexpected error when retrieving object", zap.Error(err))
			return c.Status(fiber.StatusInternalServerError).JSON(APIError{
				Message: err.Error(),
			})
		}

		return c.Status(fiber.StatusOK).
			Send(resp.Object)
	}
}

// NewUpdateObjectHandler godoc
// @Summary  Update an object by id. This will completely replace an objects contents.
// @Tags     Objects
// @Accept   */*
// @Success  200  "Successfully updated object to new content."
// @Failure  500  {object}  APIError
// @Param    id   path      string  true  "Object ID"
// @Router   /index/{id}/object [put]
func NewUpdateObjectHandler(s *sakuin.Service) fiber.Handler {
	return func(c *fiber.Ctx) error {
		id := c.Params("id")

		_, err := s.UpdateObject(c.Context(), &sakuin.UpdateObjectRequest{
			ID:      id,
			Content: c.Body(),
		})
		if _, ok := err.(sakuin.ObjectDoesNotExistErr); ok {
			zap.L().Error("object does not exist", zap.String("id", id))
			return c.SendStatus(fiber.StatusNotFound)
		}
		if err != nil {
			zap.L().Error("unexpected error when updating object", zap.Error(err))
			return c.Status(fiber.StatusInternalServerError).JSON(APIError{
				Message: err.Error(),
			})
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

// NewGetMetadataHandler godoc
// @Summary  Retrieve metadata for an object.
// @Tags     Metadata
// @Accept   json
// @Produce  json
// @Success  200  {object}  map[string]interface{}
// @Param    id   path      string  true  "Object ID"
// @Router   /index/{id}/metdata [get]
func NewGetMetadataHandler(s *sakuin.Service) fiber.Handler {
	return func(c *fiber.Ctx) error {
		id := c.Params("id")

		resp, err := s.GetMetadata(c.Context(), &sakuin.GetMetadataRequest{
			ID: id,
		})
		if _, ok := err.(sakuin.DocumentDoesNotExistErr); ok {
			zap.L().Error("metadata does not exist", zap.String("id", id))
			return c.SendStatus(fiber.StatusNotFound)
		}
		if err != nil {
			zap.L().Error("unexpected error when retrieving metadata", zap.Error(err))
			return err
		}

		return c.Status(fiber.StatusOK).
			JSON(resp.Metadata)
	}
}

// NewIndexHandler godoc
// @Summary  index a new object along with its metadata
// @Tags     Index
// @Accept   multipart/form-data
// @Produce  json
// @Param    metadata  body      map[string]interface{}  true  "Object metadata"
// @Success  200       {object}  sakuin.IndexResponse
// @Failure  400       {object}  APIError
// @Failure  500       {object}  APIError
// @Router   /index [post]
func NewIndexHandler(s *sakuin.Service) fiber.Handler {
	return func(c *fiber.Ctx) error {
		metadata, object, err := sakuin.ReadParts(bytes.NewReader(c.Body()), c.Get("Content-Type"))
		if err != nil {
			if cerr, ok := err.(sakuin.ContentTypeError); ok {
				zap.L().Error("invalid content type", zap.String("content-type", cerr.ContentType))

				return c.Status(fiber.StatusBadRequest).JSON(APIError{
					Message: cerr.Error(),
				})
			}

			zap.L().Error("unexpected error when reading request body", zap.Error(err))
			return c.Status(fiber.StatusInternalServerError).JSON(APIError{
				Message: err.Error(),
			})
		}
		if object == nil {
			return c.Status(fiber.StatusBadRequest).JSON(ErrMissingObjectPart)
		}

		resp, err := s.Index(c.Context(), &sakuin.IndexRequest{
			Metadata: metadata,
			Object:   object,
		})
		if err != nil {
			zap.L().Error("unexpected error when indexing", zap.Error(err))
			return err
		}

		return c.Status(fiber.StatusOK).
			JSON(resp)
	}
}
