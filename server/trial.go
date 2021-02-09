package server

import (
	"os"
	"time"

	"github.com/beevik/ntp"
	"github.com/pilosa/pilosa/v2"
)

func (m *Command) trialVersion() {
	if pilosa.TrialDeadline != "" {
		endTime, err := time.Parse("2006-01-02", pilosa.TrialDeadline)
		if err != nil {
			m.logger.Printf("parsing curTime from make file: %v", err)
			os.Exit(1)
		}
		go m.dailyCheck(endTime)
	}
}

const hoursPerCheck = 24 * time.Hour

// dailyCheck runs in the background while a trial version of Molecula is being run, displaying daily reminders of the remaining days
func (m *Command) dailyCheck(endTime time.Time) {
	ticker := time.NewTicker(hoursPerCheck)
	startTime, err := m.ntpServerTime(4)
	if err != nil {
		m.logger.Printf("reading ntp server time %v", err)
		os.Exit(1)
	}
	runDuration := endTime.Sub(startTime)
	if runDuration <= 0 {
		m.logger.Printf("Trial edition of Molecula has expired, exiting now!")
		os.Exit(1)
	}
	for range ticker.C {
		runningDuration := time.Since(startTime)
		m.logger.Printf("Current time remaining in trial: %v", runDuration-runningDuration)
		if runningDuration >= runDuration {
			m.logger.Printf("Trial edition of Molecula has expired, exiting now!")
			os.Exit(1)
		}
	}
}

const url = "0.beevik-ntp.pool.ntp.org"
const timeRequestDelay = 100 * time.Millisecond

// ntpServerTime attempts to reach ntp servers with delays between each attempt, returning the time value of the first connected server
func (m *Command) ntpServerTime(retries int) (time.Time, error) {
	t, err := ntp.Time(url)
	if err != nil && retries <= 0 {
		return t, err
	}
	if err != nil {
		time.Sleep(timeRequestDelay)
		return m.ntpServerTime(retries - 1)
	}
	return t, nil
}
