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

func (db *BoltStorage) GetEdges(e blend.Edge) ([]blend.Edge, error) {
	edges := []blend.Edge{}

	if len(e.Family) == 0 {
		e.Family = "public"
	}

	err := db.store.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("edge")).Cursor()

		// format for edge key:
		// vertexFromId:family:type:name

		id := e.From + ":" + e.Family
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

func (db *BoltStorage) AddVertex(v *blend.Vertex) error {
	vbytes, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return db.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))
		vertexBucket.Put([]byte(v.Id), vbytes)

		return nil
	})
}

func (db *BoltStorage) UpdateVertex(v *blend.Vertex) error {
	vbytes, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return db.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))
		vertexBucket.Put([]byte(v.Id), vbytes)
		return nil
	})
}

func (db *BoltStorage) AddEdge(e *blend.Edge) error {
	edges, err := GetEdges(blend.Edge{
		From:   e.From,
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

	return db.store.Update(func(tx *bolt.Tx) error {
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

func (db *BoltStorage) DeleteVertex(v *blend.Vertex) error {
	return db.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))

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

	backEdges, err := backend.GetEdges(blend.Edge{From: vertex.Id, Family: "ownership"})

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
