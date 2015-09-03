// Package notify enables independent components of an application to
// observe notable events in a decoupled fashion.
//
// It generalizes the pattern of *multiple* consumers of an event (ie:
// the same message delivered to multiple channels) and obviates the need
// for components to have intimate knowledge of each other (only `import notify`
// and the name of the event are shared).
//
// Example:
//     // producer of "my_event"
//     go func() {
//         for {
//             time.Sleep(time.Duration(1) * time.Second):
//             notify.Post("my_event", time.Now().Unix())
//         }
//     }()
//
//     // observer of "my_event" (normally some independent component that
//     // needs to be notified when "my_event" occurs)
//     myEventChan := make(chan interface{})
//     notify.Start("my_event", myEventChan)
//     go func() {
//         for {
//             data := <-myEventChan
//             log.Printf("MY_EVENT: %#v", data)
//         }
//     }()
package notify

import (
	"errors"
	"sync"
	"time"
)

const E_NOT_FOUND = "E_NOT_FOUND"

// returns the current version
func Version() string {
	return "0.2"
}

// internal mapping of event names to observing channels
var events = make(map[string][]chan interface{})

// mutex for touching the event map
var rwMutex sync.RWMutex

// Start observing the specified event via provided output channel
func Start(event string, outputChan chan interface{}) {
	rwMutex.Lock()
	defer rwMutex.Unlock()

	events[event] = append(events[event], outputChan)
}

// Stop observing the specified event on the provided output channel
func Stop(event string, outputChan chan interface{}) error {
	rwMutex.Lock()
	defer rwMutex.Unlock()

	newArray := make([]chan interface{}, 0)
	outChans, ok := events[event]
	if !ok {
		return errors.New(E_NOT_FOUND)
	}
	for _, ch := range outChans {
		if ch != outputChan {
			newArray = append(newArray, ch)
		} else {
			close(ch)
		}
	}
	events[event] = newArray

	return nil
}

// Stop observing the specified event on all channels
func StopAll(event string) error {
	rwMutex.Lock()
	defer rwMutex.Unlock()

	outChans, ok := events[event]
	if !ok {
		return errors.New(E_NOT_FOUND)
	}
	for _, ch := range outChans {
		close(ch)
	}
	delete(events, event)

	return nil
}

// Post a notification (arbitrary data) to the specified event
func Post(event string, data interface{}) error {
	rwMutex.RLock()
	defer rwMutex.RUnlock()

	outChans, ok := events[event]
	if !ok {
		return errors.New(E_NOT_FOUND)
	}
	for _, outputChan := range outChans {
		outputChan <- data
	}

	return nil
}

// Post a notification to the specified event using the provided timeout for
// any output channels that are blocking
func PostTimeout(event string, data interface{}, timeout time.Duration) error {
	rwMutex.RLock()
	defer rwMutex.RUnlock()

	outChans, ok := events[event]
	if !ok {
		return errors.New(E_NOT_FOUND)
	}
	for _, outputChan := range outChans {
		select {
		case outputChan <- data:
		case <-time.After(timeout):
		}
	}

	return nil
}
