package companion

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const photoFileLimit = 49 << 20
const photoMetadataLimit = 1 << 20

var errPhotoMetadata = errors.New("invalid or oversized photo metadata")

// PhotoMetadata excludes people, serials, comments and opaque MakerNotes.
type PhotoMetadata struct {
	Format         string  `json:"format"`
	Width          int     `json:"width,omitempty"`
	Height         int     `json:"height,omitempty"`
	HasEXIF        bool    `json:"has_exif"`
	HasICC         bool    `json:"has_icc"`
	HasGPS         bool    `json:"has_gps"`
	GPS            string  `json:"gps,omitempty"`
	Make           string  `json:"make,omitempty"`
	Model          string  `json:"model,omitempty"`
	Lens           string  `json:"lens,omitempty"`
	Software       string  `json:"software,omitempty"`
	DateOriginal   string  `json:"date_original,omitempty"`
	DateDigitized  string  `json:"date_digitized,omitempty"`
	DateModified   string  `json:"date_modified,omitempty"`
	OffsetOriginal string  `json:"offset_original,omitempty"`
	Exposure       string  `json:"exposure,omitempty"`
	FNumber        float64 `json:"f_number,omitempty"`
	FocalLength    float64 `json:"focal_length,omitempty"`
	ISO            int     `json:"iso,omitempty"`
	Orientation    int     `json:"orientation,omitempty"`
}

func (m PhotoMetadata) TechnicalRows() []string {
	var rows []string

	for _, field := range []struct{ key, value string }{
		{"Make", m.Make}, {"Model", m.Model}, {"LensModel", m.Lens},
		{"Software", m.Software}, {"DateTimeOriginal", m.DateOriginal},
		{"DateTimeDigitized", m.DateDigitized}, {"DateTime", m.DateModified},
		{"OffsetTimeOriginal", m.OffsetOriginal}, {"ExposureTime", m.Exposure},
	} {
		if field.value != "" {
			rows = append(rows, field.key+": "+field.value)
		}
	}
	if m.FNumber > 0 {
		rows = append(rows, "FNumber: "+strconv.FormatFloat(m.FNumber, 'f', -1, 64))
	}
	if m.FocalLength > 0 {
		rows = append(rows, "FocalLength: "+strconv.FormatFloat(m.FocalLength, 'f', -1, 64))
	}
	if m.ISO > 0 {
		rows = append(rows, fmt.Sprintf("ISO: %d", m.ISO))
	}
	if m.Orientation > 0 {
		rows = append(rows, fmt.Sprintf("Orientation: %d", m.Orientation))
	}
	return rows
}

type photoMetaParser struct {
	r     io.ReaderAt
	size  int64
	reads int
	meta  PhotoMetadata
}

func (p *photoMetaParser) read(offset int64, size int) ([]byte, error) {
	if offset < 0 || size < 0 || size > photoMetadataLimit || offset > p.size-int64(size) || p.reads > photoMetadataLimit-size {
		return nil, errPhotoMetadata
	}
	p.reads += size
	b := make([]byte, size)
	if _, err := p.r.ReadAt(b, offset); err != nil {
		return nil, errPhotoMetadata
	}
	return b, nil
}

// ReadPhotoMetadata reads bounded headers and whitelisted TIFF entries only.
// Partial container information is returned alongside malformed-metadata errors.
func ReadPhotoMetadata(path string) (PhotoMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return PhotoMetadata{}, errors.New("photo unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 2 || info.Size() > photoFileLimit {
		return PhotoMetadata{}, errPhotoMetadata
	}
	p := photoMetaParser{r: f, size: info.Size()}
	b, err := p.read(0, int(min(int64(12), p.size)))
	if err != nil {
		return p.meta, err
	}
	switch {
	case len(b) >= 12 && string(b[:4]) == "RIFF" && string(b[8:12]) == "WEBP":
		p.meta.Format = "WebP"
		err = p.webp(b)
	case len(b) >= 8 && bytes.Equal(b[:8], []byte("\x89PNG\r\n\x1a\n")):
		p.meta.Format = "PNG"
		err = p.png()
	case b[0] == 0xff && b[1] == 0xd8:
		p.meta.Format = "JPEG"
		err = p.jpeg()
	default:
		err = errors.New("unsupported photo format")
	}
	return p.meta, err
}

func (p *photoMetaParser) webp(header []byte) error {
	end := int64(binary.LittleEndian.Uint32(header[4:8])) + 8
	if end != p.size {
		return errPhotoMetadata
	}
	for offset, chunks := int64(12), 0; offset < end; chunks++ {
		if chunks >= 8192 {
			return errPhotoMetadata
		}
		h, err := p.read(offset, 8)
		if err != nil {
			return err
		}
		n := int64(binary.LittleEndian.Uint32(h[4:]))
		start := offset + 8
		if n > end-start || n+n%2 > end-start {
			return errPhotoMetadata
		}
		switch string(h[:4]) {
		case "EXIF":
			p.meta.HasEXIF = true
			if err = p.tiff(start, n); err != nil {
				return err
			}
		case "ICCP":
			p.meta.HasICC = true
			if n > photoMetadataLimit {
				return errPhotoMetadata
			}
		case "VP8X":
			if n != 10 {
				return errPhotoMetadata
			}
			b, e := p.read(start, 10)
			if e != nil {
				return e
			}
			p.meta.Width = int(b[4]) + int(b[5])<<8 + int(b[6])<<16 + 1
			p.meta.Height = int(b[7]) + int(b[8])<<8 + int(b[9])<<16 + 1
		case "VP8 ":
			if n < 10 {
				return errPhotoMetadata
			}
			b, e := p.read(start, 10)
			if e != nil || !bytes.Equal(b[3:6], []byte{0x9d, 0x01, 0x2a}) {
				return errPhotoMetadata
			}
			if p.meta.Width == 0 {
				p.meta.Width = int(binary.LittleEndian.Uint16(b[6:8]) & 0x3fff)
				p.meta.Height = int(binary.LittleEndian.Uint16(b[8:10]) & 0x3fff)
			}
		case "VP8L":
			if n < 5 {
				return errPhotoMetadata
			}
			b, e := p.read(start, 5)
			if e != nil || b[0] != 0x2f {
				return errPhotoMetadata
			}
			if p.meta.Width == 0 {
				bits := binary.LittleEndian.Uint32(b[1:])
				p.meta.Width, p.meta.Height = int(bits&0x3fff)+1, int(bits>>14&0x3fff)+1
			}
		}
		offset = start + n + n%2
	}
	return p.dimensions()
}

func (p *photoMetaParser) png() error {
	for offset, chunks := int64(8), 0; offset < p.size; chunks++ {
		if chunks >= 8192 {
			return errPhotoMetadata
		}
		h, err := p.read(offset, 8)
		if err != nil {
			return err
		}
		n := int64(binary.BigEndian.Uint32(h[:4]))
		start := offset + 8
		if start > p.size-4 || n > p.size-start-4 {
			return errPhotoMetadata
		}
		switch string(h[4:]) {
		case "IHDR":
			if chunks != 0 || n != 13 {
				return errPhotoMetadata
			}
			b, e := p.read(start, 8)
			if e != nil {
				return e
			}
			w, h := binary.BigEndian.Uint32(b[:4]), binary.BigEndian.Uint32(b[4:])
			if w > 0x7fffffff || h > 0x7fffffff {
				return errPhotoMetadata
			}
			p.meta.Width, p.meta.Height = int(w), int(h)
		case "eXIf":
			p.meta.HasEXIF = true
			if err = p.tiff(start, n); err != nil {
				return err
			}
		case "iCCP":
			p.meta.HasICC = true
			if n > photoMetadataLimit {
				return errPhotoMetadata
			}
		case "IEND":
			if n != 0 {
				return errPhotoMetadata
			}
			return p.dimensions()
		}
		offset = start + n + 4 // CRC is skipped; this is not a full image integrity check.
	}
	return errPhotoMetadata
}

func (p *photoMetaParser) jpeg() error {
	r := bufio.NewReaderSize(io.NewSectionReader(p.r, 2, p.size-2), 4096)
	offset := int64(2)
	for markers := 0; markers < 8192; {
		b, err := r.ReadByte()
		offset++
		if err != nil {
			return errPhotoMetadata
		}
		if b != 0xff {
			continue // Entropy-coded scan bytes are never decoded or retained.
		}
		for b == 0xff {
			b, err = r.ReadByte()
			offset++
			if err != nil {
				return errPhotoMetadata
			}
		}
		if b == 0 || b == 1 || b >= 0xd0 && b <= 0xd8 {
			continue
		}
		markers++
		if b == 0xd9 {
			return p.dimensions()
		}
		var length [2]byte
		if _, err = io.ReadFull(r, length[:]); err != nil {
			return errPhotoMetadata
		}
		offset += 2
		n := int(binary.BigEndian.Uint16(length[:])) - 2
		if n < 0 || int64(n) > p.size-offset {
			return errPhotoMetadata
		}
		if b == 0xe1 && n >= 6 {
			h, e := p.read(offset, 6)
			if e != nil {
				return e
			}
			if bytes.Equal(h, []byte("Exif\x00\x00")) {
				p.meta.HasEXIF = true
				if err = p.tiff(offset+6, int64(n-6)); err != nil {
					return err
				}
			}
		}
		if b == 0xe2 && n >= 12 {
			h, e := p.read(offset, 12)
			if e != nil {
				return e
			}
			p.meta.HasICC = p.meta.HasICC || bytes.Equal(h, []byte("ICC_PROFILE\x00"))
		}
		if b >= 0xc0 && b <= 0xcf && b != 0xc4 && b != 0xc8 && b != 0xcc {
			if n < 6 {
				return errPhotoMetadata
			}
			h, e := p.read(offset, 5)
			if e != nil {
				return e
			}
			p.meta.Height, p.meta.Width = int(binary.BigEndian.Uint16(h[1:3])), int(binary.BigEndian.Uint16(h[3:5]))
		}
		if _, err = r.Discard(n); err != nil {
			return errPhotoMetadata
		}
		offset += int64(n)
	}
	return errPhotoMetadata
}

func (p *photoMetaParser) dimensions() error {
	if p.meta.Width <= 0 || p.meta.Height <= 0 {
		return errPhotoMetadata
	}
	return nil
}

func (p *photoMetaParser) tiff(start, size int64) error {
	if size < 8 || size > photoMetadataLimit {
		return errPhotoMetadata
	}
	read := func(offset uint32, n int) ([]byte, error) {
		if uint64(offset)+uint64(n) > uint64(size) {
			return nil, errPhotoMetadata
		}
		return p.read(start+int64(offset), n)
	}
	header, err := read(0, 8)
	if err != nil {
		return err
	}
	if bytes.Equal(header[:6], []byte("Exif\x00\x00")) {
		start, size = start+6, size-6
		header, err = read(0, 8)
		if err != nil {
			return err
		}
	}
	var order binary.ByteOrder
	switch string(header[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return errPhotoMetadata
	}
	if order.Uint16(header[2:4]) != 42 {
		return errPhotoMetadata
	}
	type ifd struct {
		offset uint32
		depth  int
	}
	pending := []ifd{{order.Uint32(header[4:8]), 0}}
	visited := map[uint32]bool{}
	entries := 0
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current.offset == 0 {
			continue
		}
		if current.depth > 4 || len(visited) >= 16 || visited[current.offset] {
			return errPhotoMetadata
		}
		visited[current.offset] = true
		b, e := read(current.offset, 2)
		if e != nil {
			return e
		}
		count := int(order.Uint16(b))
		entries += count
		if entries > 512 || uint64(current.offset)+2+uint64(count)*12+4 > uint64(size) {
			return errPhotoMetadata
		}
		for i := 0; i < count; i++ {
			entry, e := read(current.offset+2+uint32(i)*12, 12)
			if e != nil {
				return e
			}
			tag, fieldType := order.Uint16(entry[:2]), order.Uint16(entry[2:4])
			n, value := order.Uint32(entry[4:8]), order.Uint32(entry[8:12])
			if tag == 0x8825 {
				if fieldType == 4 && n == 1 && value != 0 {
					p.meta.HasGPS = true
					if err := p.gpsIFD(read, order, value, size, visited, &entries); err != nil {
						return err
					}
				}
				continue
			}
			if tag == 0x8769 && fieldType == 4 && n == 1 {
				pending = append(pending, ifd{value, current.depth + 1})
				continue
			}
			var target *string
			switch tag {
			case 0x010f:
				target = &p.meta.Make
			case 0x0110:
				target = &p.meta.Model
			case 0xa434:
				target = &p.meta.Lens
			case 0x0131:
				target = &p.meta.Software
			case 0x9003:
				target = &p.meta.DateOriginal
			case 0x9004:
				target = &p.meta.DateDigitized
			case 0x0132:
				target = &p.meta.DateModified
			case 0x9011:
				target = &p.meta.OffsetOriginal
			}
			if target != nil && fieldType == 2 && n > 0 && n <= 128 {
				text := entry[8 : 8+min(n, 4)]
				if n > 4 {
					text, e = read(value, int(n))
					if e != nil {
						return e
					}
				}
				textValue := photoTechnicalString(text)
				if tag == 0x9003 || tag == 0x9004 || tag == 0x0132 {
					if _, e := time.Parse("2006:01:02 15:04:05", textValue); e != nil || len(textValue) != 19 {
						textValue = ""
					}
				}
				if tag == 0x9011 && !photoKnownOffset(textValue) {
					textValue = ""
				}
				if *target == "" {
					*target = textValue
				}
			}
			if n == 1 && (fieldType == 3 || fieldType == 4) {
				v := value
				if fieldType == 3 {
					v = uint32(order.Uint16(entry[8:10]))
				}
				if tag == 0x0112 && v >= 1 && v <= 8 {
					p.meta.Orientation = int(v)
				}
				if (tag == 0x8827 || tag == 0x8833) && v > 0 && v <= 10000000 && p.meta.ISO == 0 {
					p.meta.ISO = int(v)
				}
			}
			if n == 1 && fieldType == 5 && (tag == 0x829a || tag == 0x829d || tag == 0x920a) {
				raw, e := read(value, 8)
				if e != nil {
					return e
				}
				numerator, denominator := order.Uint32(raw[:4]), order.Uint32(raw[4:])
				if numerator == 0 || denominator == 0 {
					continue
				}
				v := float64(numerator) / float64(denominator)
				switch tag {
				case 0x829a:
					if v <= 86400 {
						p.meta.Exposure = fmt.Sprintf("%d/%d s", numerator, denominator)
					}
				case 0x829d:
					if v <= 1024 {
						p.meta.FNumber = v
					}
				case 0x920a:
					if v <= 100000 {
						p.meta.FocalLength = v
					}
				}
			}
		}
		next, e := read(current.offset+2+uint32(count)*12, 4)
		if e != nil {
			return e
		}
		pending = append(pending, ifd{order.Uint32(next), current.depth + 1})
	}
	return nil
}

func (p *photoMetaParser) gpsIFD(read func(uint32, int) ([]byte, error), order binary.ByteOrder, offset uint32, size int64, visited map[uint32]bool, entries *int) error {
	if offset == 0 {
		return nil
	}
	if len(visited) >= 16 || visited[offset] {
		return errPhotoMetadata
	}
	visited[offset] = true
	b, err := read(offset, 2)
	if err != nil {
		return err
	}
	count := int(order.Uint16(b))
	*entries += count
	if *entries > 512 || uint64(offset)+2+uint64(count)*12+4 > uint64(size) {
		return errPhotoMetadata
	}
	var latRef, lonRef float64
	var lat, lon float64
	var hasLat, hasLon bool
	for i := 0; i < count; i++ {
		entry, e := read(offset+2+uint32(i)*12, 12)
		if e != nil {
			return e
		}
		tag, fieldType := order.Uint16(entry[:2]), order.Uint16(entry[2:4])
		n, value := order.Uint32(entry[4:8]), order.Uint32(entry[8:12])
		switch tag {
		case 0x0001:
			if fieldType == 2 && n >= 1 && n <= 4 {
				switch entry[8] {
				case 'N', 'n':
					latRef = 1
				case 'S', 's':
					latRef = -1
				}
			}
		case 0x0003:
			if fieldType == 2 && n >= 1 && n <= 4 {
				switch entry[8] {
				case 'E', 'e':
					lonRef = 1
				case 'W', 'w':
					lonRef = -1
				}
			}
		case 0x0002:
			v, ok, e := gpsCoord(read, order, fieldType, n, value, 90)
			if e != nil {
				return e
			}
			if ok {
				lat, hasLat = v, true
			}
		case 0x0004:
			v, ok, e := gpsCoord(read, order, fieldType, n, value, 180)
			if e != nil {
				return e
			}
			if ok {
				lon, hasLon = v, true
			}
		}
	}
	if hasLat && hasLon && latRef != 0 && lonRef != 0 && (lat != 0 || lon != 0) {
		p.meta.GPS = formatGPSCoord(lon*lonRef) + ", " + formatGPSCoord(lat*latRef)
	}
	return nil
}

func gpsCoord(read func(uint32, int) ([]byte, error), order binary.ByteOrder, fieldType uint16, n, offset uint32, limit float64) (float64, bool, error) {
	if fieldType != 5 || n < 1 || n > 3 {
		return 0, false, nil
	}
	raw, err := read(offset, int(n)*8)
	if err != nil {
		return 0, false, err
	}
	var total float64
	scale := 1.0
	for i := 0; i < int(n); i++ {
		num := order.Uint32(raw[i*8 : i*8+4])
		den := order.Uint32(raw[i*8+4 : i*8+8])
		if den == 0 {
			return 0, false, nil
		}
		part := float64(num) / float64(den)
		if part < 0 || (i > 0 && part > 60) {
			return 0, false, nil
		}
		total += part / scale
		scale *= 60
	}
	if total > limit || math.IsNaN(total) || math.IsInf(total, 0) {
		return 0, false, nil
	}
	return total, true, nil
}

func formatGPSCoord(v float64) string {
	rounded := math.Round(v*1e6) / 1e6
	if rounded == 0 {
		rounded = 0
	}
	return strconv.FormatFloat(rounded, 'f', -1, 64)
}

func photoTechnicalString(raw []byte) string {
	text := strings.TrimSpace(strings.TrimRight(string(raw), "\x00"))
	if !utf8.ValidString(text) || utf8.RuneCountInString(text) > 96 {
		return ""
	}
	for _, r := range text {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ""
		}
	}
	return text
}

func photoKnownOffset(s string) bool {
	if len(s) != 6 || s[0] != '+' && s[0] != '-' || s[3] != ':' || s == "-00:00" {
		return false
	}
	for _, i := range []int{1, 2, 4, 5} {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	hours := int(s[1]-'0')*10 + int(s[2]-'0')
	minutes := int(s[4]-'0')*10 + int(s[5]-'0')
	return hours <= 14 && minutes < 60 && (hours != 14 || minutes == 0)
}
