// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Package server contains the `pilosa server` subcommand which runs Pilosa
// itself. The purpose of this package is to define an easily tested Command
// object which handles interpreting configuration and setting up all the
// objects that Pilosa needs.

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
			m.logger.Printf("parsing trial deadline: %v", err)
			os.Exit(1)
		}
		go m.dailyCheck(endTime)
	}
}

const trialCheckInterval = 24 * time.Hour

// dailyCheck runs in the background while a trial version of Molecula is being run, displaying daily reminders of the remaining days
func (m *Command) dailyCheck(endTime time.Time) {
	ticker := time.NewTicker(trialCheckInterval)
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

const ntpurl = "0.beevik-ntp.pool.ntp.org"
const ntpRetryDelay = 100 * time.Millisecond

// ntpServerTime attempts to reach ntp servers with delays between each attempt, returning the time value of the first connected server
func (m *Command) ntpServerTime(retries int) (time.Time, error) {
	t, err := ntp.Time(ntpurl)
	if err != nil && retries <= 0 {
		return t, err
	}
	if err != nil {
		time.Sleep(ntpRetryDelay)
		return m.ntpServerTime(retries - 1)
	}
	return t, nil
}
