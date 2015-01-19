package db

import "testing"

func testVertexTree(t *testing.T) {

	vertex := &blend.Vertex{
		Name:       "TestRoot",
		Type:       "test",
		Public:     "Sample data ... ",
		Private:    "secret data",
		PrivateKey: "test key",
	}

	err := AddVertex(vertex)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if ConfirmVertex(vertex.Id) == false {
		t.Error("Vertex couold not be confirmed after adding to the db")
		return
	}

	childVertex := &blend.Vertex{
		Name:       "TestChild",
		Type:       "test",
		Public:     "Sample data ... ",
		Private:    "secret data",
		PrivateKey: "test key",
	}

	childEdge := &blend.Edge{
		Type: "child",
		Name: "testedge",
		From: vertex.Id,
		Data: "test data",
	}

	err = AddVertexChild(childVertex, childEdge)
	if err != nil {
		t.Error(err.Error())
	}

	if ConfirmVertex(childVertex.Id) == false {
		t.Error("Vertex couold not be confirmed after adding to the db")
		return
	}

	edges, err := GetEdges(blend.Edge{From: vertex.Id, Family: "ownership"})
	if err != nil {
		t.Error(err.Error())
		return
	}

	if len(edges) != 1 {
		t.Error("Got more edges then expected")
		return
	}

	if edges[0].Name != childEdge.Name || edges[0].To != childVertex.Id {
		t.Error("got back different edge then expected")
		return
	}

	err = DeleteVertex(vertex)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if ConfirmVertex(vertex.Id) || ConfirmVertex(childVertex.Id) {
		t.Error("Could not delete the entire vertex tree")
		return
	}
}

func TestMemory(t *testing.T) {
	err := Init("", &MemoryStorage{})
	if err != nil {
		t.Error(err.Error())
		return
	}

	defer Close()

	testVertexTree(t)
	testAddDel(t)
}

func TestBolt(t *testing.T) {
	err := Init("./test.bolt.db", &BoltStorage{})
	if err != nil {
		t.Error(err.Error())
		return
	}

	defer Close()

	testVertexTree(t)
	testAddDel(t)
}

func testAddDel(t *testing.T) {
	vertex := &blend.Vertex{
		Name:       "TestAdd",
		Type:       "test",
		Public:     "Sample data ... ",
		Private:    "secret data",
		PrivateKey: "test key",
	}

	err := AddVertex(vertex)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if ConfirmVertex(vertex.Id) == false {
		t.Error("Vertex couold not be confirmed after adding to the db")
		return
	}

	err = DeleteVertex(vertex)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if ConfirmVertex(vertex.Id) == true {
		t.Error("Vertex couold not be deleted for some reason")
		return
	}
}
