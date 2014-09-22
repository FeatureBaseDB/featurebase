package index

import (
	"time"
)

type TimeQuantum uint

const (
	Y    TimeQuantum = 3
	YM   TimeQuantum = 2
	YMD  TimeQuantum = 1
	YMDH TimeQuantum = 0
)

func GetTimeID(t TimeQuantum, year uint, month uint, day uint, hour uint, tile_id uint64) uint64 {
	y := year - 1970 //config.startyear
	time_stamp := uint64((uint(t) << 30) | (y << 23) | (month << 19) | (day << 14) | (hour << 9))
	time_stamp = time_stamp << 32
	result := time_stamp | tile_id

	return result
}

func GetTimeIds(tile_id uint64, atime time.Time, min_quantum TimeQuantum) []uint64 {
	results := make([]uint64, 0, 4)
	year := uint(atime.Year())
	month := atime.Month()
	day := atime.Day()
	hour := atime.Hour()

	if min_quantum <= Y {
		id := GetTimeID(Y, year, 0, 0, 0, tile_id)
		results = append(results, id)
	}

	if min_quantum <= YM {
		id := GetTimeID(YM, uint(year), uint(month), 0, 0, tile_id)
		results = append(results, id)
	}

	if min_quantum <= YMD {
		id := GetTimeID(YMD, year, uint(month), uint(day), 0, tile_id)
		results = append(results, id)
	}

	if min_quantum <= YMDH {
		id := GetTimeID(YMDH, year, uint(month), uint(day), uint(hour), tile_id)
		results = append(results, id)
	}

	return results
}

func IncrementYear(t time.Time) time.Time {
	return t.AddDate(1, 0, 0)
}
func IncrementMonth(t time.Time) time.Time {
	return t.AddDate(0, 1, 0)
}
func IncrementDay(t time.Time) time.Time {
	return t.AddDate(0, 0, 1)
}
func IncrementHour(t time.Time) time.Time {
	return t.Add(time.Hour)
}

func sameYear(t1, t2 time.Time) bool {
	y1, _, _ := t1.Date()
	y2, _, _ := t2.Date()
	return (y1 == y2)
}

func NextYear(start time.Time, end time.Time) bool {
	nextYear := start.AddDate(1, 0, 0)
	return sameYear(nextYear, end) || end.After(nextYear)
}

func sameMonth(t1, t2 time.Time) bool {
	y1, m1, _ := t1.Date()
	y2, m2, _ := t2.Date()
	return (y1 == y2) && (m1 == m2)
}

func NextMonth(start time.Time, end time.Time) bool {
	nextMonth := start.AddDate(0, 1, 0)
	return sameMonth(nextMonth, end) || end.After(nextMonth)
}
func sameDay(t1, t2 time.Time) bool {
	y1, m1, d1 := t1.Date()
	y2, m2, d2 := t2.Date()
	return (y1 == y2) && (m1 == m2) && (d1 == d2)
}
func NextDay(start time.Time, end time.Time) bool {
	nextDay := start.AddDate(0, 0, 1)
	return sameDay(nextDay, end) || end.After(nextDay)
}
func GetRange(start_time time.Time, end_time time.Time, tile_id uint64) []uint64 {
	results, marker := upHill(start_time, end_time, tile_id)
	r2 := downHill(marker, end_time, tile_id)
	results = append(results, r2...)
	return results
}

func upHill(start_time time.Time, end_time time.Time, tile_id uint64) ([]uint64, time.Time) {
	var results []uint64
	time_iterator := start_time
	for time_iterator.Before(end_time) {
		if NextDay(time_iterator, end_time) {
			if time_iterator.Hour() == 0 {
				if NextMonth(time_iterator, end_time) {
					if time_iterator.Day() == 1 {
						if NextYear(time_iterator, end_time) {
							if time_iterator.Month() == 1 {
								break
							} else {

								month_id := GetTimeID(YM, uint(time_iterator.Year()), uint(time_iterator.Month()), 0, 0, tile_id)
								results = append(results, month_id)
								time_iterator = IncrementMonth(time_iterator)
							}
						} else {
							break
						}
					} else {
						day_id := GetTimeID(YMD, uint(time_iterator.Year()), uint(time_iterator.Month()), uint(time_iterator.Day()), 0, tile_id)
						results = append(results, day_id)
						time_iterator = IncrementDay(time_iterator)
					}
				} else {
					break
				}
			} else {
				hour_id := GetTimeID(YMDH, uint(time_iterator.Year()), uint(time_iterator.Month()), uint(time_iterator.Day()), uint(time_iterator.Hour()), tile_id)
				results = append(results, hour_id)
				time_iterator = IncrementHour(time_iterator)
			}
		} else {
			break
		}
	}
	return results, time_iterator

}
func downHill(start_time time.Time, end_time time.Time, tile_id uint64) []uint64 {

	var results []uint64
	time_iterator := start_time
	for time_iterator.Before(end_time) {
		if NextYear(time_iterator, end_time) {
			year_id := GetTimeID(Y, uint(time_iterator.Year()), 0, 0, 0, tile_id)
			results = append(results, year_id)
			time_iterator = IncrementYear(time_iterator)
		} else if NextMonth(time_iterator, end_time) {
			month_id := GetTimeID(YM, uint(time_iterator.Year()), uint(time_iterator.Month()), 0, 0, tile_id)
			results = append(results, month_id)
			time_iterator = IncrementMonth(time_iterator)
		} else if NextDay(time_iterator, end_time) {
			day_id := GetTimeID(YMD, uint(time_iterator.Year()), uint(time_iterator.Month()), uint(time_iterator.Day()), 0, tile_id)
			results = append(results, day_id)
			time_iterator = IncrementDay(time_iterator)
		} else {
			hour_id := GetTimeID(YMDH, uint(time_iterator.Year()), uint(time_iterator.Month()), uint(time_iterator.Day()), uint(time_iterator.Hour()), tile_id)
			results = append(results, hour_id)
			time_iterator = IncrementHour(time_iterator)
		}
	}
	return results
}
