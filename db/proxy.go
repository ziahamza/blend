package db

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/ziahamza/blend"
)

// HTTP API Backend
type ProxyStorage struct {
	url   *url.URL
	cache Storage
}

func (db *ProxyStorage) Init(uri string) error {
	var err error

	db.url, err = url.Parse(uri)
	if err != nil {
		return err
	}

	// check if the graph api is working.
	resp, err := http.Get(db.url.String())
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var response struct {
		GraphVersion string `json:"graph-version"`
		Success      bool   `json:"success"`
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}

	if !response.Success || response.GraphVersion != "" {
		return errors.New("Cannot parse graph API for proxy backend")
	}

	db.cache = &BoltStorage{}

	db.cache.Init(path.Join(os.TempDir(), "proxycache.db"))

	return nil
}

func (db *ProxyStorage) Close() {
	db.cache.Close()
}

func (db *ProxyStorage) Drop() error {
	return db.cache.Drop()
}

func (db *ProxyStorage) GetAPIResponse(req blend.APIRequest) blend.APIResponse {

	return blend.APIResponse{}
}

func (db *ProxyStorage) GetVertex(v *blend.Vertex) error {
	err := db.cache.GetVertex(v)
	if err == nil {
		// vertex from cache found, return early
		return nil
	}
	relURI, err := url.Parse("/vertex/" + v.Id)
	if err != nil {
		return err
	}

	if v.PrivateKey != "" {
		q := url.Values{}
		q.Set("private_key", v.PrivateKey)
		relURI.RawQuery = q.Encode()
	}

	// resp, err := http.Get(db.url.ResolveReference(relURI).String())

	return nil
}

func (db *ProxyStorage) GetEdges(e blend.Edge) ([]blend.Edge, error) {
	return nil, nil
}

func (db *ProxyStorage) AddVertex(v *blend.Vertex) error {
	return nil
}

func (db *ProxyStorage) UpdateVertex(v *blend.Vertex) error {
	return nil
}

func (db *ProxyStorage) AddEdge(e *blend.Edge) error {
	return nil
}

func (db *ProxyStorage) DeleteVertex(v *blend.Vertex) error {
	return nil
}

func (db *ProxyStorage) DeleteVertexTree(vertices []*blend.Vertex) error {
	return nil
}
