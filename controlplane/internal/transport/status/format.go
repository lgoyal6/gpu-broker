package status

import "strconv"

func fmtPct(f float64) string { return strconv.FormatFloat(f*100, 'f', 1, 64) + "%" }
func fmtInt(n int64) string   { return strconv.FormatInt(n, 10) }
