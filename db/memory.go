package db

import (
	"errors"
	"fmt"
	"os"
)

func fillVertex(v1, v2 *Vertex, private bool) {
	v2.Id = v1.Id
	v2.LastChanged = v1.LastChanged
	v2.Name = v1.Name
	v2.Type = v1.Type
	v2.Public = v1.Public

	if private {
		v2.Private = v1.Private
		v2.PrivateKey = v1.PrivateKey
	}
}

type Node struct {
	vertex Vertex
	edges  []Edge
}

type MemoryStorage struct {
	store *os.File
	nodes map[string]*Node
}

func (db *MemoryStorage) Init(path string) error {

	if len(path) == 0 {
		path = "/tmp/blend.db"
	}

	/*
		db.store, err := os.Create(path)
		if err != nil {
			return err
		}
	*/

	db.nodes = make(map[string]*Node)

	return nil
}

func (db *MemoryStorage) Close() {
	db.store.Close()
}

func (db *MemoryStorage) Drop() error {
	// for now do nothing

	return nil
}

func (db *MemoryStorage) getNode(id string) *Node {
	node, ok := db.nodes[id]
	if ok == false {
		return nil
	}

	return node
}

func (db *MemoryStorage) addNode(vertex *Vertex) *Node {
	node := Node{
		vertex: *vertex,
		edges:  []Edge{},
	}

	db.nodes[vertex.Id] = &node

	return &node
}

func (db *MemoryStorage) delNode(vertex *Vertex) error {
	if db.getNode(vertex.Id) == nil {
		return errors.New("Unknown vertex id given for deletion")
	}

	delete(db.nodes, vertex.Id)

	return nil
}

func (db *MemoryStorage) GetEdges(e Edge) ([]Edge, error) {
	edges := []Edge{}
	node := db.getNode(e.From)
	if node == nil {
		return nil, errors.New("")
	}

	for _, edge := range node.edges {
		if edge.Family != e.Family {
			continue
		}

		if (len(e.Type) > 0) && edge.Type != e.Type {
			continue
		}

		if (len(e.Name) > 0) && edge.Name != e.Name {
			continue
		}

		edges = append(edges, edge)
	}

	return edges, nil
}

func (db *MemoryStorage) GetVertex(v *Vertex, private bool) error {
	node := db.getNode(v.Id)
	if node == nil {
		return errors.New("Vertex not Found!")
	}

	fillVertex(&node.vertex, v, private)

	return nil
}

func (db *MemoryStorage) UpdateVertex(v *Vertex) error {
	oldVertex := &Vertex{Id: v.Id}
	err := db.GetVertex(oldVertex, true)
	if err != nil {
		return err
	}

	v.PrivateKey = oldVertex.PrivateKey
	fillVertex(v, oldVertex, true)

	return nil
}

func (db *MemoryStorage) AddVertex(v *Vertex) error {
	_ = db.addNode(v)
	return nil
}

func (db *MemoryStorage) AddEdge(edge *Edge) error {
	edges, err := GetEdges(Edge{
		From:   edge.From,
		Family: edge.Family,
		Name:   edge.Name,
		Type:   edge.Type,
	})

	if err == nil && len(edges) > 0 {
		edge.To = edges[0].To

		fmt.Println("edge found already, returning the old edge: ", edge.Family, ":", edge.Type, ":", edge.Name)
		return nil
	}

	node := db.getNode(edge.From)
	if node == nil {
		return errors.New("Vertex not found!")
	}

	node.edges = append(node.edges, *edge)

	return nil
}

func (db *MemoryStorage) DeleteVertex(v *Vertex) error {
	return db.delNode(v)
}
