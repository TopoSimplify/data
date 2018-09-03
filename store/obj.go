package store

import "math"

type Obj struct {
	mt  *MTraffic
	dt  float64
	ddt float64
}

func NewObj(o *MTraffic) *Obj {
	return &Obj{mt: o, dt: 0, ddt: 0}
}

func (self *Obj) Dt() float64 {
	return self.dt
}
func (self *Obj) Ddt() float64 {
	return self.ddt
}
func (self *Obj) Mt() *MTraffic {
	return self.mt
}

func (b *Obj) Delta(a *Obj) *Obj {
	b.dt = b.mt.Time.Sub(a.mt.Time).Hours()
	return b
}

func (b *Obj) DDelta(a *Obj) *Obj {
	b.ddt = math.Abs(math.Abs(b.dt) - math.Abs(a.dt))
	return b
}
