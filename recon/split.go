package recon

import (
	. "simplex/data/store"
	"math"
)
const null = -9999

//split tracjectories into consistent segments
func SplitTraj(pings []*MTraffic) [][]*MTraffic {
	trajectories := make([][]*MTraffic, 0)
	traj := make([]*MTraffic, 0)
	prvdt := null

	var fn_flush = func() {
		trajectories = append(trajectories, traj)
		traj = make([]*MTraffic, 0)
	}

	for i := range pings {
		if i == 0 {
			traj = append(traj, pings[i])
		} else {
			n := len(traj)
			a, b := traj[n - 1], pings[i]
			dt := TimeDelta(a, b)
			if prvdt == null {
				prvdt = dt
			}
			ddt := math.Abs(math.Abs(prvdt) - dt)

			if (dt < 0.5) && (ddt < 0.1) {
				traj = append(traj, b)
			} else {
				fn_flush()
				traj = append(traj, b)
			}
		}
	}

	if len(traj) > 0 {
		fn_flush()
	}

	return trajectories
}

