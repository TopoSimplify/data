package store

import (
	"fmt"
	"github.com/boltdb/bolt"
	"strconv"
)

//string to byte
func B(s string) []byte {
	return []byte(s)
}

//int to byte
func ItoB(i int) []byte {
	return B(strconv.Itoa(i))
}

//next id
func NextId(b *bolt.Bucket) ([]byte, error) {
	id, err := b.NextSequence()
	if err != nil {
		return nil, err
	}
	return B(strconv.FormatUint(id, 10)), nil
}

//create id
func createTaskID(id interface{}) string {
	return fmt.Sprintf("%v", id)
}

//put string
func PutS(b *bolt.Bucket, key string, value string) error {
	return Put(b, B(key), B(value))
}

//put
func Put(b *bolt.Bucket, key []byte, value []byte) error {
	return b.Put(key, value)
}

//is error
func IsErr(err error) bool {
	return err != nil
}

func Shift(self *[][]*Obj) []*Obj {
	this := *self
	x, this := this[0], this[1:]
	*self = this
	return x
}

func UnShift(a []*Obj, b []*Obj) []*Obj {
	_a := a[0:len(a):len(a)]
	return append(_a, b...)
}

func UnShiftQueue(queue *[][]*Obj, b []*Obj) [][]*Obj {
	_b := [][]*Obj{b}
	*queue = append(_b, (*queue)...)
	return *queue
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
	copy(a[i:], a[i+1:])
	a[len(a)-1] = nil // or the zero value of T
	*self = a[:len(a)-1]
	return x
}

func Push(self *[]*MTraffic, x *MTraffic) int {
	*self = append(*self, x)
	return len(*self)
}

func ExtendFrom(self *[]*MTraffic, i int, other []*MTraffic) *[]*MTraffic {
	b := other[0:len(other):len(other)]
	a := *self
	*self = append(a[:i], append(b, a[i:]...)...)
	return self
}

func Extend(a []*Obj, b []*Obj) []*Obj {
	return append(a[0:len(a):len(a)], b...)
}

func Reverse(self *[]*MTraffic) *[]*MTraffic {
	a := *self
	for l, r := 0, len(a)-1; l < r; l, r = l+1, r-1 {
		a[l], a[r] = a[r], a[l]
	}
	*self = a
	return self
}

func Len(self []*Obj) int {
	return len(self)
}

func IsEmpty(o []*Obj) bool {
	return Len(o) == 0
}

func First(t []*Obj) *Obj {
	if !IsEmpty(t) {
		return t[0]
	}
	return nil
}

func Last(t []*Obj) *Obj {
	if !IsEmpty(t) {
		return t[len(t)-1]
	}
	return nil
}

func ShiftQueue(self [][]*MTraffic) ([]*MTraffic, [][]*MTraffic) {
	if len(self) == 0 {
		return []*MTraffic{}, [][]*MTraffic{}
	}
	return self[0], self[1:]
}
