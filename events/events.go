package events

import (
	"github.com/ziahamza/blend/db"
	"sync"
	"sync/atomic"
)

type VertexListener struct {
	events chan db.Event

	// number of listeners listening on the events channel
	count int32
}

var dispatcher struct {
	sync.RWMutex
	listeners map[string]*VertexListener
}

func Init() {
	dispatcher.listeners = make(map[string]*VertexListener)
}

func getVertexListener(id string) *VertexListener {
	dispatcher.RLock()
	defer dispatcher.RUnlock()

	return dispatcher.listeners[id]
}

func delVertexListener(id string) bool {
	dispatcher.Lock()
	defer dispatcher.Unlock()

	listener := dispatcher.listeners[id]
	if listener == nil {
		return false
	}

	if atomic.LoadInt32(&listener.count) > 0 {
		return false
	}

	atomic.StoreInt32(&listener.count, -1)
	close(listener.events)

	delete(dispatcher.listeners, id)

	return true
}

// tries to add a vertex listener to the id, if it already
// exists then returns the old one, else adds a new one
func setVertexListener(id string, listener *VertexListener) *VertexListener {
	dispatcher.Lock()
	defer dispatcher.Unlock()

	if dispatcher.listeners[id] != nil {
		return dispatcher.listeners[id]
	}

	dispatcher.listeners[id] = listener

	return listener
}

func Dispatch(id string, event db.Event) {
	listener := getVertexListener(id)

	if listener != nil && atomic.LoadInt32(&listener.count) > 0 {
		listener.events <- event
	}
}

func Subscribe(id string) chan db.Event {
	listener := getVertexListener(id)

	if listener == nil {
		listener = setVertexListener(id, &VertexListener{})
	}

	atomic.AddInt32(&listener.count, 1)

	return listener.events
}

func Unsubscribe(id string) {
	listener := getVertexListener(id)

	if listener != nil {
		val := atomic.AddInt32(&listener.count, -1)
		if val <= 0 {
			// vertex events not used anymore
			if delVertexListener(id) == false {
				// cant really delete the vertex for some reason
				// ignore for now
			}
		}
	}
}
