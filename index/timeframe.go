package index

import "time"

type TimeQuantum uint

const (
	Y TimeQuantum = 0
	YM
	YMD
	YMDH
)

func PackTimeBits(t TimeQuantum, year uint, month uint, day uint, hour uint) uint64 {
	v := uint64((uint(t) << 30) | (year << 24) | (month << 20) | (day << 15) | (hour << 10))
	v = v << 32
	return v
}

func getTimeIds(tile_id uint64, atime time.Time, min_quantum TimeQuantum, offset uint) []uint64 {
	results := make([]uint64, 0, 4)
	year := uint(atime.Year()) - offset
	month := atime.Month()
	day := atime.Day()
	hour := atime.Hour()

	if min_quantum <= Y {
		id := PackTimeBits(Y, year, 0, 0, 0)
		id = id | tile_id
		results = append(results, id)
	}

	if min_quantum <= YM {
		id := PackTimeBits(YM, uint(year), uint(month), 0, 0)
		id = id | tile_id
		results = append(results, id)
	}

	if min_quantum <= YMD {
		id := PackTimeBits(YMD, year, uint(month), uint(day), 0)
		id = id | tile_id
		results = append(results, id)
	}

	if min_quantum <= YMDH {
		id := PackTimeBits(Y, year, uint(month), uint(day), uint(hour))
		id = id | tile_id
		results = append(results, id)
	}

	return results
}
