package events

import "github.com/ziahamza/blend/db"
import "sync"

type VertexListener struct {
	Vertex *db.Vertex
	Events chan *db.Event

	// number of listeners listening on the events channel
	numListeners int
}

var dispatcher struct {
	sync.RWMutex
	listeners map[string]*VertexListener
}

func Init() {
	dispatcher.listeners = make(map[string]*VertexListener)
}

func Subscribe(vertex *db.Vertex) *VertexListener {
	return nil
}

func Unsubscribe(v *VertexListener) {

}
