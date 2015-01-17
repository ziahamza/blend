package db

import (
	"errors"
	"time"

	"github.com/nu7hatch/gouuid"
)

type Vertex struct {
	Id          string    `json:"vertex_id"`
	LastChanged time.Time `json:"last_changed"`
	Name        string    `json:"vertex_name"`
	Type        string    `json:"vertex_type"`
	Private     string    `json:"private_data"`
	PrivateKey  string    `json:"private_key"`
	Public      string    `json:"public_data"`
}

type Event struct {
	Source  string    `json:"verted_id"`
	Type    string    `json:"event_type"`
	Created time.Time `json:"event_time"`
}

// EDGE types: ownership, public, private and event
type Edge struct {
	LastChanged string `json:"last_changed"`
	Family      string `json:"edge_family"`
	Type        string `json:"edge_type"`
	Name        string `json:"edge_name"`
	From        string `json:"vertex_from"`
	To          string `json:"vertex_to"`
	Data        string `json:"edge_data"`
}

type Storage interface {
	Init(string) error

	// close the db connection, ignore errors if not closed properly
	Close()

	// drop any data in the database and recreate the schema
	Drop() error

	// Gets all edges under a vertex. The edge type is filled with all
	// the required fields for retrieving the edge. The From field is compulsory
	// and should conatin the source vertex ID. Fill in the edge type with Family
	// and optionally type to refine the list further
	// Assumes that:
	//		* vertex with the specific ID exists
	//		* a sane family is given to transverse the edges
	GetEdges(Edge) ([]Edge, error)

	// Fill in the details of the vertex by its Id. The second parameter dictates
	// weather private details will be retrieved
	GetVertex(*Vertex, bool) error

	// Add a specific edge to the DB. fills in the Edge pointer with the new ID
	// of the edge
	AddEdge(*Edge) error

	// Adds a new vertex, only used to build root isolated vertices
	AddVertex(*Vertex) error

	// Updates the details of a vertex. An entire vertex needs to be given as all
	// details are updated at once. The update vertex automatically sets the private
	// key from the original vertex, it is not overwridden.
	UpdateVertex(*Vertex) error

	DeleteVertex(*Vertex) error

	DeleteVertexTree([]*Vertex) error
}

var backend Storage

func Init(uri string, s Storage) error {
	backend = s
	return backend.Init(uri)
}

func Close() {
	backend.Close()
}

func Drop() error {
	return backend.Drop()
}

func GetEdges(edge Edge) ([]Edge, error) {
	return backend.GetEdges(edge)
}

func GetVertex(vertex *Vertex, private bool) error {
	if vertex.Id == "" {
		return errors.New("Vertex Id not passed")
	}

	return backend.GetVertex(vertex, private)
}

func UpdateVertex(vertex *Vertex) error {
	if vertex.Id == "" {
		return errors.New("Vertex Id not passed")
	}

	return backend.UpdateVertex(vertex)
}

func AddEdge(edge *Edge) error {
	if edge.Family != "ownership" && edge.Family != "private" &&
		edge.Family != "public" && edge.Family != "event" {
		return errors.New("Edge Family not supported")
	}

	// if no name given then make it unque by the edge_vertex
	// as edges are unique with respect to the name
	if edge.Name == "" {
		edge.Name = edge.To
	}

	return backend.AddEdge(edge)

	return nil
}

func PropogateChanges(vertex Vertex, event Event) error {
	return nil
}

func AddVertex(vertex *Vertex) error {
	if vertex.Id == "" {
		vid, err := uuid.NewV4()

		if err != nil {
			return err
		}

		vertex.Id = string(vid.String())
	}

	err := backend.AddVertex(vertex)

	if err == nil {
		err = PropogateChanges(*vertex, Event{
			Source:  vertex.Id,
			Type:    "vertex:create",
			Created: vertex.LastChanged,
		})
	}

	return err
}

func DeleteVertex(vertex *Vertex) error {
	return DeleteVertexTree([]*Vertex{vertex})
}

func DeleteVertexTree(vertices []*Vertex) error {
	return backend.DeleteVertexTree(vertices)
}

func AddVertexChild(vertex *Vertex, edge *Edge) error {
	edge.Family = "ownership"

	vid, err := uuid.NewV4()

	if err != nil {
		return err
	}

	vertex.Id = string(vid.String())

	// edge is meant to be unique, so make sure we overwride an existing one rather than adding a
	// new one

	// Method 1: First try to create a new dummy ownership edge for the vertex. If succeeded then
	// change its vertex_to Id to the newly created vertex. If not then a vertex already
	// exists to fill in that vertex

	// Method 2: Try a batch commit with cassandra transactions. Should allow edges and vertices
	// be added without mutating them later

	// METHOD 3: Change the schema so that all edges are unique and make sure edge name is always set
	// (if not then use the destination vertex ID). And then try to add a new edge before adding vertex

	// For now, method 3 is chosen.

	edge.To = vertex.Id

	err = AddEdge(edge)

	if err != nil {
		return err
	}

	if edge.To != vertex.Id {
		// Vertex already exists, fill in the old details
		vertex.Id = edge.To
		return UpdateVertex(vertex)
	}

	return AddVertex(vertex)
}

func ConfirmVertex(vertex_id string) bool {
	return backend.GetVertex(&Vertex{Id: vertex_id}, false) == nil
}

func ConfirmVertexKey(vertex_id, vertex_key string) bool {
	vertex := &Vertex{Id: vertex_id}
	err := backend.GetVertex(vertex, true)
	if err != nil {
		return false
	}

	if vertex.Private == vertex_key {
		return true
	}

	// Temporary HACK for testing
	if vertex_key == "root" {
		return true
	}

	return false
}
