// boltdb backend
package db

import "github.com/boltdb/bolt"
import "time"

type BoltStorage struct {
	store *bolt.DB
	path  string
}

func (db *BoltStorage) Init(path string) error {
	var err error

	if len(path) == 0 {
		path = "/tmp/blend.db"
	}

	db.path = path

	db.store, err = bolt.Open(path, 0666, &bolt.Options{
		Timeout: 5 * time.Second,
	})

	return err
}

func (db *BoltStorage) Close() {
	db.store.Close()
}

func (db *BoltStorage) Drop() error {
	// for now do nothing
	db.Close()

	return nil
}

/*
func (db *BoltStorage) getVertex(id string) *db.Vertex {

}

func (db *BoltStorage) getNode(id string) *Node {
	node, ok := db.nodes[id]
	if ok == false {
		return nil
	}

	return node
}

func (db *BoltStorage) addNode(vertex *Vertex) *Node {
	node := Node{
		vertex: *vertex,
		edges:  []Edge{},
	}

	db.nodes[vertex.Id] = &node

	return &node
}

func (db *BoltStorage) delNode(vertex *Vertex) error {
	if db.getNode(vertex.Id) == nil {
		return errors.New("Unknown vertex id given for deletion")
	}

	delete(db.nodes, vertex.Id)

	return nil
}

func (db *BoltStorage) GetEdges(e Edge) ([]Edge, error) {
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

func (db *BoltStorage) GetVertex(v *Vertex, private bool) error {
	node := db.getNode(v.Id)
	if node == nil {
		return errors.New("Vertex not Found!")
	}

	fillVertex(&node.vertex, v, private)

	return nil
}

func (db *BoltStorage) UpdateVertex(v *Vertex) error {
	oldVertex := &Vertex{Id: v.Id}
	err := db.GetVertex(oldVertex, true)
	if err != nil {
		return err
	}

	v.PrivateKey = oldVertex.PrivateKey
	fillVertex(v, oldVertex, true)

	return nil
}

func (db *BoltStorage) AddVertex(v *Vertex) error {
	_ = db.addNode(v)
	return nil
}

func (db *BoltStorage) AddEdge(edge *Edge) error {
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

func (db *BoltStorage) DeleteVertex(v *Vertex) error {
	return db.delNode(v)
}
*/
