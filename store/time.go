package store

func TimeDelta(a, b *MTraffic) float64 {
	dt := b.Time.Sub(a.Time)
	return dt.Hours()
}
