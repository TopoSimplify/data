package recon

import . "simplex/data/store"


func  Shift(self *[]*MTraffic) *MTraffic {
	this := *self
	x, this := this[0], this[1:]
	*self = this
	return x
}

func  UnShift(self *[]*MTraffic, x *MTraffic) int {
	*self = append([]*MTraffic{x}, *self...)
	return len(*self)
}

func Pop(self *[]*MTraffic) *MTraffic {
	var a = *self
	var x *MTraffic
	var n = len(a) - 1
	x, *self = a[n], a[:n]
	return x
}

func Delete(self *[]*MTraffic, i int) *MTraffic {
	a := *self
	x := a[i]
	copy(a[i:], a[i + 1:])
	a[len(a) - 1] = nil // or the zero value of T
	*self = a[:len(a) - 1]
	return x
}

func  Push(self *[]*MTraffic, x *MTraffic) int {
	*self = append(*self, x)
	return len(*self)
}

func  Extend(self *[]*MTraffic, i int, other []*MTraffic) *[]*MTraffic {
	b := other[0 :len(other):len(other)]
	a := *self
	*self = append(a[:i], append(b, a[i:]...)...)
	return self
}

func  Reverse(self *[]*MTraffic) *[]*MTraffic {
	a := *self
	for l, r := 0, len(a) - 1; l < r; l, r = l + 1, r - 1 {
		a[l], a[r] = a[r], a[l]
	}
	*self = a
	return self
}

func  Len(self []*MTraffic) int {
	return len(self)
}

func  IsEmpty(self []*MTraffic) bool {
	return Len(self) == 0
}

func First(t []*MTraffic) *MTraffic{
	if !IsEmpty(t) {
		return t[0]
	}
	return nil
}

func Last(t []*MTraffic) *MTraffic{
	if !IsEmpty(t) {
		return t[len(t) - 1]
	}
	return nil
}


func ShiftQueue(self [][]*MTraffic ) ([]*MTraffic, [][]*MTraffic) {
	if len(self) == 0 {
		return []*MTraffic{}, [][]*MTraffic{}
	}
	return self[0], self[1:]
}
