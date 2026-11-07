package companion

import (
	"encoding/hex"
	"errors"
	"strings"
)

func archiveMediaName(sha, mime string) string {
	mime = strings.ToLower(strings.TrimSpace(strings.Split(mime, ";")[0]))
	ext := map[string]string{"image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png", "image/apng": "png", "image/webp": "webp", "image/gif": "gif", "image/avif": "avif", "image/svg+xml": "svg", "image/tiff": "tiff", "image/bmp": "bmp", "image/x-ms-bmp": "bmp", "image/heic": "heic", "image/heif": "heif", "image/x-icon": "ico", "image/vnd.microsoft.icon": "ico"}[mime]
	if ext == "" {
		ext = "bin"
	}
	return "media/" + sha + "." + ext
}

// Accept old snapshots, but new ZIPs use an extension for ordinary file viewers.
func archiveMediaSHA(name string) (string, error) {
	if !strings.HasPrefix(name, "media/") {
		return "", errors.New("unexpected media archive path")
	}
	base := strings.TrimPrefix(name, "media/")
	parts := strings.Split(base, ".")
	if len(parts) > 2 || len(parts[0]) != 64 || strings.ToLower(parts[0]) != parts[0] {
		return "", errors.New("invalid media archive filename")
	}
	if _, err := hex.DecodeString(parts[0]); err != nil {
		return "", errors.New("invalid media archive hash")
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "jpg", "jpeg", "png", "webp", "gif", "avif", "svg", "tif", "tiff", "bmp", "heic", "heif", "ico", "bin":
		default:
			return "", errors.New("unsupported media archive extension")
		}
	}
	return parts[0], nil
}
