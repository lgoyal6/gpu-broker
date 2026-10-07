package carbon

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// pyDumps reproduces Python's json.dumps(obj, sort_keys=True) for the value
// shapes in a dataset (objects, arrays, numbers, strings, null), so the Go
// loader can verify the checksum the Python fetcher wrote.
func pyDumps(v any) string {
	var b strings.Builder
	write(&b, v)
	return b.String()
}

func write(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			b.WriteString(strconv.FormatInt(int64(x), 10))
		} else {
			b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
		}
	case string:
		b.WriteString(strconv.Quote(x))
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteString(", ")
			}
			write(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote(k))
			b.WriteString(": ")
			write(b, x[k])
		}
		b.WriteByte('}')
	default:
		panic(fmt.Sprintf("pyDumps: unsupported %T", v))
	}
}
