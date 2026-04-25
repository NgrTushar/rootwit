// Package logger provides a global structured logger for RootWit.
// Call Init() once in main before using L anywhere else.
package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// L is the global sugared logger. Use L.Infow/Warnw/Errorw with key-value pairs.
// Defaults to a no-op logger so it is safe to use before Init() is called
// (e.g. from package init functions or TestMain). Init() replaces it with the
// real configured logger.
var L = zap.NewNop().Sugar()

// Init initialises the global logger. JSON format when stdout is not a TTY
// (CI, Docker, log aggregators); human-readable console format otherwise.
func Init() {
	var core zapcore.Core

	if isTTY() {
		// Console format for local development.
		enc := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
		core = zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), zapcore.DebugLevel)
	} else {
		// JSON for production / log aggregators (Datadog, CloudWatch, etc.)
		enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		core = zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	}

	L = zap.New(core).Sugar()
}

func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
