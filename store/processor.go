package store

//import (
//  . "simplex/struct/rtree"
//  . "simplex/data/store"
//)

//
//func processor(stash []*MTraffic, queue [][]*MTraffic, db *RTree) {
//  stash.Push(Shift(queue));
//  stash.Push(Shift(queue));
//
//
//  var first = First(stash);
//  var last  = Last(stash);
//
//  var a     = Last(first);
//  var b     = First(last);
//  var c     *MTraffic ;
//  if   len(queue) == 1 {
//    c = queue[0][0]
//  } //c is first of first item in queue
//
//  var args  = []*MTraffic{first, last, a, b, c};
//  //only one in queue
//  var boolcase0   = len(last) == 0 && len(queue) == 0;
//  //different trajs
//  var boolcase00    = true
//
//  //pings from the same vessel
//  // .........a | b........|c......
//  ///meminter
//  if (boolcase0) {
//    case0(jargs, first);
//  } else if (boolcase00) {
//    case00(jargs, first);
//  } else {
//    inter([]*MTraffic{a, b}, db, caseinter(jargs, args));
//  }
//}
