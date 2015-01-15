package db

// HTTP API Backend

type ProxyStorage struct {
	uri string
}

func (db *ProxyStorage) Init(uri string) error {
	return nil
}

func (db *ProxyStorage) Close() {

}

func (db *ProxyStorage) Drop() error {
	return nil
}

func (db *ProxyStorage) GetVertex(v *Vertex, private bool) error {
	return nil
}

func (db *ProxyStorage) GetEdges(e Edge) ([]Edge, error) {
	return nil, nil
}

func (db *ProxyStorage) AddVertex(v *Vertex) error {
	return nil
}

func (db *ProxyStorage) UpdateVertex(v *Vertex) error {
	return nil
}

func (db *ProxyStorage) AddEdge(e *Edge) error {
	return nil
}

func (db *ProxyStorage) DeleteVertex(v *Vertex) error {
	return nil
}
