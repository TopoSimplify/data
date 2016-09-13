package recon

import . "simplex/data/store"

type Traffic []*MTraffic

func (self *Traffic) Shift() *MTraffic {
	this := *self
	x, this := this[0], this[1:]
	*self = this
	return x
}

func (self *Traffic) UnShift(x *MTraffic) int {
	*self = append(Traffic{x}, *self...)
	return len(*self)
}

func (self *Traffic) Pop() *MTraffic {
	var a = *self
	var x *MTraffic
	var n = len(a) - 1
	x, *self = a[n], a[:n]
	return x
}

func (self *Traffic)Delete(i *MTraffic) *MTraffic {
	a := *self
	x := a[i]
	copy(a[i:], a[i + 1:])
	a[len(a) - 1] = nil // or the zero value of T
	*self = a[:len(a) - 1]
	return x
}

func (self *Traffic) Push(x *MTraffic) int {
	*self = append(*self, x)
	return len(*self)
}

func (self *Traffic) Extend(i *MTraffic, other Traffic) *Traffic {
	b := other[0 :len(other):len(other)]
	a := *self
	*self = append(a[:i], append(b, a[i:]...)...)
	return self
}

func (self *Traffic) Reverse() *Traffic {
	a := *self
	for l, r := 0, len(a) - 1; l < r; l, r = l + 1, r - 1 {
		a[l], a[r] = a[r], a[l]
	}
	*self = a
	return self
}

func (self *Traffic) Len() int {
	return len(*self)
}

func (self *Traffic) IsEmpty() bool {
	return self.Len() == 0
}

func First(t Traffic) *MTraffic{
	if !t.IsEmpty() {
		return t[0]
	}
	return nil
}

func Last(t Traffic) *MTraffic{
	if !t.IsEmpty() {
		return t[len(t) - 1]
	}
	return nil
}


func Shift(self []Traffic ) Traffic {
	this := *self
	if len(this) == 0 {
		return Traffic{}
	}
	x, this := this[0], this[1:]
	*self = this
	return x
}
