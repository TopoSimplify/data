package store


import (
    "time"
    "bytes"
    "strconv"
    "strings"
)

func ParseInt(v string) (int, error) {
    var vv = bytes.Trim([]byte(v), "\xef\xbb\xbf")
    return strconv.Atoi(string(vv))
}

func ParseFloat(v string) (float64, error) {
    var vv = bytes.Trim([]byte(v), "\xef\xbb\xbf")
    return strconv.ParseFloat(string(vv), 64)
}

func ParseTime(v string) (time.Time, error) {
    if !strings.Contains(v, "T") {
        tokens := strings.Split(v, " ")
        v = strings.Join(tokens, "T")
    }
    if !strings.Contains(v, "Z") {
        v = v + "Z"
    }
    vv, _ := time.Parse(time.RFC3339, v)
    return vv, nil
}
