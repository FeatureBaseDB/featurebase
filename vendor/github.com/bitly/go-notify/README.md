## go-notify

Package notify enables independent components of an application to
observe notable events in a decoupled fashion.

It generalizes the pattern of *multiple* consumers of an event (ie: the
same message delivered to multiple channels) and obviates the need for
components to have intimate knowledge of each other (only `import
notify` and the name of the event are shared).

Example:

    // producer of "my_event"
    go func() {
        for {
            time.Sleep(time.Duration(1) * time.Second):
            notify.Post("my_event", time.Now().Unix())
        }
    }()

    // observer of "my_event" (normally some independent component that
    // needs to be notified when "my_event" occurs)
    myEventChan := make(chan interface{})
    notify.Start("my_event", myEventChan)
    go func() {
        for {
            data := <-myEventChan
            log.Printf("MY_EVENT: %#v", data)
        }
    }()

### Functions

    func Post(event string, data interface{}) error
        Post a notification (arbitrary data) to the specified event

    func PostTimeout(event string, data interface{}, timeout time.Duration) error
        Post a notification to the specified event using the provided timeout for
        any output channels that are blocking

    func Start(event string, outputChan chan interface{})
        Start observing the specified event via provided output channel

    func Stop(event string, outputChan chan interface{}) error
        Stop observing the specified event on the provided output channel

    func StopAll(event string) error
        Stop observing the specified event on all channels

    func Version() string
        returns the current version
