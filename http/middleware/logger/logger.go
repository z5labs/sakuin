// Package logger
package logger

import (
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func New() fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		path := ctx.Path()
		zap.L().Debug("request received", zap.String("path", path)) // TODO: log body
		zap.L().Info("request received", zap.String("path", path))

		return nil
	}
}
