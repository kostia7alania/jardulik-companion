package companion

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type photoMetaRange struct{ start, end int64 }
type photoMetaGuard struct {
	data      []byte
	forbidden []photoMetaRange
	violated  bool
}

func (g *photoMetaGuard) ReadAt(b []byte, offset int64) (int, error) {
	for _, span := range g.forbidden {
		if offset < span.end && offset+int64(len(b)) > span.start {
			g.violated = true
			return 0, io.ErrUnexpectedEOF
		}
	}
	return bytes.NewReader(g.data).ReadAt(b, offset)
}

func photoMetaTIFF(order binary.ByteOrder) ([]byte, []photoMetaRange) {
	type entry struct {
		tag, kind uint16
		count     uint32
		value     uint32
		data      []byte
		private   bool
	}
	ascii := func(tag uint16, text string, private bool) entry {
		return entry{tag: tag, kind: 2, count: uint32(len(text) + 1), data: append([]byte(text), 0), private: private}
	}
	rational := func(tag uint16, n, d uint32) entry {
		b := make([]byte, 8)
		order.PutUint32(b, n)
		order.PutUint32(b[4:], d)
		return entry{tag: tag, kind: 5, count: 1, data: b}
	}
	root := []entry{ascii(0x010f, "TestMake", false), ascii(0x0110, "TestModel", false), ascii(0x0131, "TestEditor", false), {tag: 0x0112, kind: 3, count: 1, value: 6}, {tag: 0x8825, kind: 4, count: 1}, {tag: 0x8769, kind: 4, count: 1}, ascii(0x013b, "PRIVATE_OWNER_DO_NOT_READ", true), ascii(0x0132, "2022:09:10 11:12:13", false)}
	exif := []entry{ascii(0x9003, "2021:08:09 10:11:12", false), ascii(0x9011, "+03:00", false), ascii(0xa434, "TestLens", false), rational(0x829a, 1, 125), rational(0x829d, 28, 10), rational(0x920a, 35, 1), {tag: 0x8827, kind: 3, count: 1, value: 400}, ascii(0x927c, "PRIVATE_MAKER_NOTES_DO_NOT_READ", true), ascii(0x9286, "PRIVATE_COMMENT_DO_NOT_READ", true), ascii(0xa431, "PRIVATE_SERIAL_DO_NOT_READ", true), ascii(0x9004, "2021:08:09 10:11:14", false)}
	exifOffset := 8 + 2 + len(root)*12 + 4
	data := make([]byte, exifOffset+2+len(exif)*12+4)
	if order == binary.LittleEndian {
		copy(data, "II")
	} else {
		copy(data, "MM")
	}
	order.PutUint16(data[2:], 42)
	order.PutUint32(data[4:], 8)
	root[5].value = uint32(exifOffset)
	var forbidden []photoMetaRange
	writeIFD := func(offset int, entries []entry) {
		order.PutUint16(data[offset:], uint16(len(entries)))
		for i, e := range entries {
			at := offset + 2 + i*12
			order.PutUint16(data[at:], e.tag)
			order.PutUint16(data[at+2:], e.kind)
			order.PutUint32(data[at+4:], e.count)
			if e.data != nil {
				start := len(data)
				data = append(data, e.data...)
				order.PutUint32(data[at+8:], uint32(start))
				if e.private {
					forbidden = append(forbidden, photoMetaRange{int64(start), int64(len(data))})
				}
			} else if e.kind == 3 {
				order.PutUint16(data[at+8:], uint16(e.value))
			} else {
				order.PutUint32(data[at+8:], e.value)
			}
		}
	}
	writeIFD(8, root)
	writeIFD(exifOffset, exif)
	gps := len(data)
	data = append(data, []byte("PRIVATE_GPS_COORDINATE_VALUES_DO_NOT_READ")...)
	order.PutUint32(data[8+2+4*12+8:], uint32(gps))
	forbidden = append(forbidden, photoMetaRange{int64(gps), int64(len(data))})
	return data, forbidden
}

func photoMetaWebP(exif []byte) []byte {
	data := []byte("RIFF\x00\x00\x00\x00WEBP")
	chunk := func(kind string, payload []byte) {
		data = append(data, kind...)
		data = binary.LittleEndian.AppendUint32(data, uint32(len(payload)))
		data = append(data, payload...)
		if len(payload)%2 != 0 {
			data = append(data, 0)
		}
	}
	chunk("VP8X", []byte{0x28, 0, 0, 0, 31, 0, 0, 23, 0, 0})
	chunk("EXIF", exif)
	chunk("ICCP", []byte("test color profile"))
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)-8))
	return data
}

func photoMetaWrite(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "extensionless")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPhotoMetadataEndianAndImageContainers(t *testing.T) {
	for _, order := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
		tiff, _ := photoMetaTIFF(order)
		var jpegData bytes.Buffer
		if err := jpeg.Encode(&jpegData, image.NewGray(image.Rect(0, 0, 32, 24)), nil); err != nil {
			t.Fatal(err)
		}
		segment := func(marker byte, payload []byte) []byte {
			b := []byte{0xff, marker}
			b = binary.BigEndian.AppendUint16(b, uint16(len(payload)+2))
			return append(b, payload...)
		}
		jpg := append([]byte{0xff, 0xd8}, segment(0xe1, append([]byte("Exif\x00\x00"), tiff...))...)
		jpg = append(jpg, segment(0xe2, []byte("ICC_PROFILE\x00\x01\x01profile"))...)
		jpg = append(jpg, jpegData.Bytes()[2:]...)
		var pngData bytes.Buffer
		if err := png.Encode(&pngData, image.NewGray(image.Rect(0, 0, 32, 24))); err != nil {
			t.Fatal(err)
		}
		pngChunk := func(kind string, payload []byte) []byte {
			b := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
			b = append(b, kind...)
			b = append(b, payload...)
			return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b[4:]))
		}
		pngBytes := append([]byte(nil), pngData.Bytes()[:33]...)
		pngBytes = append(pngBytes, pngChunk("eXIf", tiff)...)
		pngBytes = append(pngBytes, pngChunk("iCCP", []byte("profile\x00\x00"))...)
		pngBytes = append(pngBytes, pngData.Bytes()[33:]...)
		for format, data := range map[string][]byte{"WebP": photoMetaWebP(tiff), "JPEG": jpg, "PNG": pngBytes} {
			t.Run(format+"_"+order.String(), func(t *testing.T) {
				m, err := ReadPhotoMetadata(photoMetaWrite(t, data))
				if err != nil {
					t.Fatal(err)
				}
				if m.Format != format || m.Width != 32 || m.Height != 24 || !m.HasEXIF || !m.HasICC || !m.HasGPS || m.Make != "TestMake" || m.Model != "TestModel" || m.Lens != "TestLens" || m.Software != "TestEditor" || m.DateOriginal != "2021:08:09 10:11:12" || m.OffsetOriginal != "+03:00" || m.Exposure != "1/125 s" || m.FNumber != 2.8 || m.FocalLength != 35 || m.ISO != 400 || m.Orientation != 6 {
					t.Fatalf("technical fields lost: %#v", m)
				}
				if m.DateDigitized != "2021:08:09 10:11:14" || m.DateModified != "2022:09:10 11:12:13" {
					t.Fatalf("distinct source date tags lost: %#v", m)
				}
				b, _ := json.Marshal(m)
				if strings.Contains(string(b), "PRIVATE_") || strings.Contains(strings.Join(m.TechnicalRows(), "\n"), "PRIVATE_") {
					t.Fatal("private metadata leaked into output")
				}
			})
		}
	}
}

func TestPhotoMetadataDoesNotReadGPSOrPrivateFieldPayloads(t *testing.T) {
	data, private := photoMetaTIFF(binary.LittleEndian)
	guard := &photoMetaGuard{data: data, forbidden: private}
	p := photoMetaParser{r: guard, size: int64(len(data))}
	if err := p.tiff(0, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if guard.violated || !p.meta.HasGPS || p.meta.Model != "TestModel" {
		t.Fatal("parser followed a private payload or lost safe fields")
	}
}

func TestPhotoMetadataRejectsOffsetsLoopsAndBudgets(t *testing.T) {
	base, _ := photoMetaTIFF(binary.LittleEndian)
	for name, mutate := range map[string]func([]byte) []byte{
		"offset":  func(b []byte) []byte { binary.LittleEndian.PutUint32(b[4:], 0xffffffff); return b },
		"entries": func(b []byte) []byte { binary.LittleEndian.PutUint16(b[8:], 0xffff); return b },
		"ifd_loop": func(b []byte) []byte {
			n := int(binary.LittleEndian.Uint16(b[8:]))
			binary.LittleEndian.PutUint32(b[8+2+n*12:], 8)
			return b
		},
		"string_offset": func(b []byte) []byte { binary.LittleEndian.PutUint32(b[18:], 0xffffffff); return b },
		"nested_prefix": func([]byte) []byte { return bytes.Repeat([]byte("Exif\x00\x00"), 1000) },
		"huge_exif":     func([]byte) []byte { return make([]byte, photoMetadataLimit+1) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := mutate(append([]byte(nil), base...))
			m, err := ReadPhotoMetadata(photoMetaWrite(t, photoMetaWebP(bad)))
			if err == nil || m.Format != "WebP" || !m.HasEXIF {
				t.Fatal("malformed metadata was accepted or partial format lost")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "huge")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(photoFileLimit + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	if _, err = ReadPhotoMetadata(path); err == nil {
		t.Fatal("oversized file accepted")
	}
	for _, data := range [][]byte{[]byte("RIFF\xff\xff\xff\xffWEBP"), []byte("RIFF\x0c\x00\x00\x00WEBPEXIF\xff\xff\xff\xff"), photoMetaWebP(base)[:20]} {
		if _, err = ReadPhotoMetadata(photoMetaWrite(t, data)); err == nil {
			t.Fatal("truncated or malicious chunk accepted")
		}
	}
}

func TestPhotoMetadataDateOffsetAndSafeText(t *testing.T) {
	for _, offset := range []string{"+03:00", "-04:30", "+00:00", "+14:00"} {
		if !photoKnownOffset(offset) {
			t.Fatalf("valid offset rejected: %s", offset)
		}
	}
	for _, offset := range []string{"", "-00:00", "+14:01", "+99:00", "+03:60", "+3:00", "+aa:00"} {
		if photoKnownOffset(offset) {
			t.Fatal("unknown offset presented as known")
		}
	}
	rows := strings.Join((PhotoMetadata{DateOriginal: "2021:08:09 10:11:12"}).TechnicalRows(), "\n")
	if !strings.Contains(rows, "DateTimeOriginal: 2021:08:09 10:11:12") || strings.Contains(rows, "OffsetTimeOriginal") || strings.Contains(rows, "UTC+") {
		t.Fatal("source date altered or missing timezone invented")
	}
	rows = strings.Join((PhotoMetadata{DateOriginal: "2026:11:08 01:20:00", OffsetOriginal: "+04:00", HasGPS: true}).TechnicalRows(), "\n")
	if !strings.Contains(rows, "DateTimeOriginal: 2026:11:08 01:20:00") || !strings.Contains(rows, "OffsetTimeOriginal: +04:00") || strings.Contains(rows, "GPS") || strings.Contains(rows, "есть") {
		t.Fatal("source tags changed or generated annotations added")
	}
	for _, raw := range []string{"camera\nowner", "camera\x00secret", "camera\u202eowner", strings.Repeat("x", 129)} {
		if photoTechnicalString([]byte(raw)) != "" {
			t.Fatal("unsafe metadata text accepted")
		}
	}
}

func TestPhotoMetadataTechnicalRowsExcludeContainerAndFlags(t *testing.T) {
	m := PhotoMetadata{Format: "WebP", Width: 900, Height: 1200, HasEXIF: true, HasGPS: true, Make: "Example", Model: "Camera", DateOriginal: "2021:08:09 10:11:12", OffsetOriginal: "+04:00", Orientation: 1}
	full := strings.Join(m.TechnicalRows(), "\n")
	for _, tag := range []string{"Make: Example", "Model: Camera", "DateTimeOriginal: 2021:08:09 10:11:12", "OffsetTimeOriginal: +04:00", "Orientation: 1"} {
		if !strings.Contains(full, tag) {
			t.Fatalf("source technical tag lost: %s", tag)
		}
	}
	for _, noise := range []string{"FileType", "ImageWidth", "ImageHeight", "GPS", "has_exif"} {
		if strings.Contains(full, noise) {
			t.Fatalf("container/flags mislabeled as EXIF: %s", noise)
		}
	}
}
