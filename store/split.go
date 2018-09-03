package store

import "github.com/intdxdt/math"

//split tracjectories into consistent segments
func SplitTraj(pings []*Obj) [][]*Obj {
	var trajectories = make([][]*Obj, 0)
	var traj         = make([]*Obj, 0)

	var fnFlush = func() {
		trajectories = append(trajectories, traj)
		traj = make([]*Obj, 0)
	}

	for i := range pings {
		if i == 0 {
			traj = append(traj, pings[i])
		} else {
			n := len(traj)
			a, b := traj[n-1], pings[i]
			b.Delta(a).DDelta(a)

			if (b.dt < 0.5) && !math.FloatEqual(a.dt, a.ddt) && (b.ddt < 0.1) {
				traj = append(traj, b)
			} else {
				fnFlush()
				traj = append(traj, b)
			}
		}
	}

	if len(traj) > 0 {
		fnFlush()
	}

	return trajectories
}
