package db

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/gorilla/websocket"
	"github.com/ziahamza/blend"
)

// HTTP API Backend
type ProxyStorage struct {
	rpcURL *url.URL
	cache  Storage
}

func (db *ProxyStorage) Init(uri string) error {
	var err error

	db.rpcURL, err = url.Parse(uri)
	if err != nil {
		return err
	}

	resp, err := db.GetAPIResponse(blend.APIRequest{Method: "/"})
	if err != nil {
		return err
	}

	if !resp.Success {
		return errors.New("Cannot parse graph API for proxy backend:" + resp.Message)
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

func (db *ProxyStorage) GetAPIResponse(req blend.APIRequest) (blend.APIResponse, error) {
	var resp blend.APIResponse
	conn, _, err := websocket.DefaultDialer.Dial(db.rpcURL.String(), http.Header{})
	if err != nil {
		return resp, err
	}

	defer conn.Close()

	err = conn.WriteJSON(&req)
	if err != nil {
		return resp, err
	}

	err = conn.ReadJSON(&resp)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (db *ProxyStorage) GetVertex(v *blend.Vertex) error {
	resp, err := db.GetAPIResponse(blend.APIRequest{
		Method: "/vertex/get",
		Vertex: *v,
	})

	if err != nil {
		return err
	}

	if resp.Success == false {
		return errors.New(resp.Message)
	}

	*v = *resp.Vertex

	return nil
}

func (db *ProxyStorage) GetEdges(v blend.Vertex, e blend.Edge) ([]blend.Edge, error) {
	resp, err := db.GetAPIResponse(blend.APIRequest{
		Method: "/edge/get",
		Vertex: v,
		Edge:   e,
	})

	if err != nil {
		return nil, err
	}

	if resp.Success == false {
		return nil, errors.New(resp.Message)
	}

	return nil, nil
}

func (db *ProxyStorage) GetChildVertex(v blend.Vertex, e blend.Edge) (blend.Vertex, error) {
	resp, err := db.GetAPIResponse(blend.APIRequest{
		Method: "/vertex/getChild",
		Vertex: v,
		Edge:   e,
	})

	if err != nil {
		return blend.Vertex{}, err
	}

	if resp.Success == false {
		return blend.Vertex{}, errors.New(resp.Message)
	}

	return *resp.Vertex, nil
}

func (db *ProxyStorage) CreateChildVertex(v, vc *blend.Vertex, e blend.Edge) error {
	resp, err := db.GetAPIResponse(blend.APIRequest{
		Method:      "/vertex/createChild",
		Vertex:      *v,
		ChildVertex: *vc,
		Edge:        e,
	})

	if err != nil {
		return err
	}

	if resp.Success == false {
		return errors.New(resp.Message)
	}

	*vc = *resp.Vertex

	return nil
}

func (db *ProxyStorage) CreateVertex(v *blend.Vertex) error {
	resp, err := db.GetAPIResponse(blend.APIRequest{
		Method: "/vertex/create",
		Vertex: *v,
	})

	if err != nil {
		return err
	}

	if resp.Success == false {
		return errors.New(resp.Message)
	}

	*v = *resp.Vertex

	return nil
}

func (db *ProxyStorage) UpdateVertex(v *blend.Vertex) error {
	return nil
}

func (db *ProxyStorage) CreateEdge(v blend.Vertex, e *blend.Edge) error {
	resp, err := db.GetAPIResponse(blend.APIRequest{
		Method: "/edge/create",
		Vertex: v,
		Edge:   *e,
	})

	if err != nil {
		return err
	}

	if resp.Success == false {
		return errors.New(resp.Message)
	}

	*e = *resp.Edge

	return nil
}

func (db *ProxyStorage) DeleteVertex(v *blend.Vertex) error {
	return nil
}

func (db *ProxyStorage) DeleteVertexTree(vertices []*blend.Vertex) error {
	return nil
}
