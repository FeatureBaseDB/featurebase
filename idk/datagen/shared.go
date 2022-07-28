package datagen

import "time"

var customers = []string{"CVS", "McDonald's", "Walmart", "Visa", "Sprint", "Comcast"}
var inStorePayments = []string{"Credit Card", "Debit Card", "Cash", "ACH", "Return"}
var ecomPayments = []string{"Credit Card", "Debit Card", "ACH", "Return"}
var tsTypes = []string{"Voltage", "VoltageRMS", "Amperage", "Resistance", "Frequency", "Message"}

var startDayInt int64
var todayInt int64

var startDate = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)
var today = time.Date(2019, time.November, 19, 0, 0, 0, 0, time.UTC)
var currentDur = today.Sub(startDate)

var daysSinceStart = int(currentDur / (time.Hour * 24))

func init() {
	epoch := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	start := time.Date(today.Year()-10, today.Month(), today.Day(), today.Hour(), today.Minute(), today.Second(), today.Nanosecond(), today.Location())
	startDayInt = int64(start.Sub(epoch) / (time.Hour * 24))
	todayInt = int64(today.Sub(epoch) / (time.Hour * 24))
}

func intptr(i int64) *int64 {
	return &i
}
