// boltdb backend
package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/boltdb/bolt"
	"github.com/ziahamza/blend"
	"os"
	"time"
)

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

	if err != nil {
		return err
	}

	err = db.store.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("vertex"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("edge"))
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

func (db *BoltStorage) Close() {
	db.store.Close()
}

func (db *BoltStorage) Drop() error {
	db.Close()

	os.Remove(db.path)

	return db.Init(db.path)
}

func (db *BoltStorage) GetVertex(v *blend.Vertex) error {
	vkey := v.PrivateKey
	return db.store.View(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))

		vbytes := vertexBucket.Get([]byte(v.Id))
		if vbytes == nil {
			return errors.New("Vertex not found.")
		}

		err := json.Unmarshal(vbytes, v)
		if err != nil {
			return err
		}

		if vkey == "" {
			v.PrivateKey = ""
		} else if v.PrivateKey != vkey {
			return errors.New("Wront private key supplied for vertex")
		}

		return nil
	})
}

func (db *BoltStorage) GetEdges(v blend.Vertex, e blend.Edge) ([]blend.Edge, error) {
	edges := []blend.Edge{}

	if len(e.Family) == 0 {
		e.Family = "public"
	}

	err := db.store.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("edge")).Cursor()

		// format for edge key:
		// vertexFromId:family:type:name

		id := v.Id + ":" + e.Family
		if e.Type != "" {
			id += ":" + e.Type

			if e.Name != "" {
				id += ":" + e.Name
			}
		}

		prefix := []byte(id)

		for k, v := cursor.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			edge := blend.Edge{}
			json.Unmarshal(v, &edge)

			edges = append(edges, edge)
		}

		return nil
	})

	return edges, err
}

func (backend *BoltStorage) GetChildVertex(v blend.Vertex, e blend.Edge) (blend.Vertex, error) {
	vertex := blend.Vertex{}
	edges, err := backend.GetEdges(v, e)

	if err != nil {
		return vertex, err
	}

	if len(edges) == 0 {
		return vertex, errors.New("Child Vertex not found!")
	}

	vertex.Id = edges[0].To
	err = GetVertex(&vertex)

	return vertex, err
}

func (backend *BoltStorage) CreateChildVertex(v, vc *blend.Vertex, e blend.Edge) error {
	e.Family = "ownership"

	vertex, err := backend.GetChildVertex(*v, e)

	if err == nil {
		vc.Id = vertex.Id
		return UpdateVertex(vc)
	}

	vbytes, err := json.Marshal(vc)
	if err != nil {
		return err
	}

	ebytes, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return backend.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))
		edgeBucket := tx.Bucket([]byte("edge"))

		vertexBucket.Put([]byte(vc.Id), vbytes)

		edgeId := e.From + ":" + e.Family + ":" + e.Type + ":" + e.Name
		edgeBucket.Put([]byte(edgeId), ebytes)

		return nil
	})
}

func (backend *BoltStorage) CreateVertex(v *blend.Vertex) error {
	vbytes, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return backend.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))
		vertexBucket.Put([]byte(v.Id), vbytes)

		return nil
	})
}

func (backend *BoltStorage) UpdateVertex(v *blend.Vertex) error {
	vbytes, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return backend.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))
		vertexBucket.Put([]byte(v.Id), vbytes)
		return nil
	})
}

func (backend *BoltStorage) CreateEdge(v blend.Vertex, e *blend.Edge) error {
	edges, err := GetEdges(v, blend.Edge{
		Family: e.Family,
		Name:   e.Name,
		Type:   e.Type,
	})

	if err == nil && len(edges) > 0 {
		e.To = edges[0].To

		// edge already found, returning the old one
		return nil
	}

	ebytes, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return backend.store.Update(func(tx *bolt.Tx) error {
		edgeBucket := tx.Bucket([]byte("edge"))
		vertexBucket := tx.Bucket([]byte("vertex"))

		if vertexBucket.Get([]byte(e.From)) == nil {
			return errors.New("The edge from vertex not found")
		}

		edgeId := e.From + ":" + e.Family + ":" + e.Type + ":" + e.Name
		edgeBucket.Put([]byte(edgeId), ebytes)

		return nil
	})
}

func (backend *BoltStorage) DeleteVertex(v *blend.Vertex) error {
	return backend.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))

		// TODO: Delete corresponding edges ...

		vertexBucket.Delete([]byte(v.Id))

		return nil
	})
}

func (backend *BoltStorage) DeleteVertexTree(vertices []*blend.Vertex) error {
	if len(vertices) == 0 {
		return nil
	}

	vertex := vertices[0]
	vertices = vertices[1:]

	backEdges, err := backend.GetEdges(*vertex, blend.Edge{Family: "ownership"})

	if err != nil {
		return err
	}

	// Breadth first deletion
	for _, edge := range backEdges {
		vertices = append(vertices, &blend.Vertex{Id: edge.To})
	}
	err = backend.DeleteVertexTree(vertices)

	if err != nil {
		return err
	}

	return backend.DeleteVertex(vertex)
}
