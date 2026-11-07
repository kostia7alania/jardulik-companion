package companion

import (
	"strings"
	"testing"
)

func TestArchiveMediaNames(t *testing.T) {
	sha := strings.Repeat("a", 64)
	for mime, ext := range map[string]string{"image/jpeg": "jpg", "image/webp": "webp", "image/png": "png", "image/avif": "avif", "image/svg+xml": "svg", "image/unknown": "bin"} {
		name := archiveMediaName(sha, mime)
		if !strings.HasSuffix(name, "."+ext) {
			t.Fatal(name)
		}
		if got, err := archiveMediaSHA(name); err != nil || got != sha {
			t.Fatalf("%s %v", got, err)
		}
	}
	if got, err := archiveMediaSHA("media/" + sha); err != nil || got != sha {
		t.Fatal("legacy archive unsupported")
	}
	for _, name := range []string{"../media/" + sha + ".jpg", "media/" + sha + ".exe", "media/" + sha + ".jpg.exe", "media/" + strings.ToUpper(sha) + ".jpg", "media/x.jpg"} {
		if _, err := archiveMediaSHA(name); err == nil {
			t.Fatalf("unsafe filename accepted: %s", name)
		}
	}
}
