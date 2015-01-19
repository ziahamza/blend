package db

import (
	"errors"

	"github.com/ziahamza/blend"

	"github.com/nu7hatch/gouuid"
)

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
	GetEdges(blend.Edge) ([]blend.Edge, error)

	// Fill in the details of the vertex by its Id.
	// The private details will only be available if the
	// private key is passed.
	GetVertex(*blend.Vertex) error

	// Add a specific edge to the DB. fills in the Edge pointer with the new ID
	// of the edge
	AddEdge(*blend.Edge) error

	// Adds a new vertex, only used to build root isolated vertices
	AddVertex(*blend.Vertex) error

	// Updates the details of a vertex. An entire vertex needs to be given as all
	// details are updated at once. The update vertex automatically sets the private
	// key from the original vertex, it is not overwridden.
	UpdateVertex(*blend.Vertex) error

	DeleteVertex(*blend.Vertex) error

	DeleteVertexTree([]*blend.Vertex) error
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

func GetEdges(edge blend.Edge) ([]blend.Edge, error) {
	return backend.GetEdges(edge)
}

func GetVertex(vertex *blend.Vertex) error {
	if vertex.Id == "" {
		return errors.New("Vertex Id not passed")
	}

	return backend.GetVertex(vertex)
}

func UpdateVertex(vertex *blend.Vertex) error {
	if vertex.Id == "" {
		return errors.New("Vertex Id not passed")
	}

	return backend.UpdateVertex(vertex)
}

func AddEdge(edge *blend.Edge) error {
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

func PropogateChanges(vertex blend.Vertex, event blend.Event) error {
	return nil
}

func AddVertex(vertex *blend.Vertex) error {
	if vertex.Id == "" {
		vid, err := uuid.NewV4()

		if err != nil {
			return err
		}

		vertex.Id = string(vid.String())
	}

	err := backend.AddVertex(vertex)

	if err == nil {
		err = PropogateChanges(*vertex, blend.Event{
			Source:  vertex.Id,
			Type:    "vertex:create",
			Created: vertex.LastChanged,
		})
	}

	return err
}

func DeleteVertex(vertex *blend.Vertex) error {
	return DeleteVertexTree([]*blend.Vertex{vertex})
}

func DeleteVertexTree(vertices []*blend.Vertex) error {
	return backend.DeleteVertexTree(vertices)
}

func AddVertexChild(vertex *blend.Vertex, edge *blend.Edge) error {
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

func ConfirmVertex(vid string) bool {
	return backend.GetVertex(&blend.Vertex{Id: vid}) == nil
}

func ConfirmVertexKey(vid, vkey string) bool {
	vertex := &blend.Vertex{Id: vid, PrivateKey: vkey}
	err := backend.GetVertex(vertex)
	if err != nil {
		return false
	}

	return true
}
