// boltdb backend
package db

import "github.com/boltdb/bolt"
import "time"
import "errors"
import "encoding/json"
import "bytes"

import "os"

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

func (db *BoltStorage) GetVertex(v *Vertex, private bool) error {
	return db.store.View(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))

		vbytes := vertexBucket.Get([]byte(v.Id))
		if vbytes == nil {
			return errors.New("Vertex not found.")
		}

		return json.Unmarshal(vbytes, v)
	})
}

func (db *BoltStorage) GetEdges(e Edge) ([]Edge, error) {
	edges := []Edge{}

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
			edge := Edge{}
			json.Unmarshal(v, &edge)

			edges = append(edges, edge)
		}

		return nil
	})

	return edges, err
}

func (db *BoltStorage) AddVertex(v *Vertex) error {
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

func (db *BoltStorage) UpdateVertex(v *Vertex) error {
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

func (db *BoltStorage) AddEdge(e *Edge) error {
	edges, err := GetEdges(Edge{
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

func (db *BoltStorage) DeleteVertex(v *Vertex) error {
	return db.store.Update(func(tx *bolt.Tx) error {
		vertexBucket := tx.Bucket([]byte("vertex"))

		vertexBucket.Delete([]byte(v.Id))

		return nil
	})
}
