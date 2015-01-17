package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"io"

	"golang.org/x/net/websocket"

	"github.com/gorilla/mux"

	"github.com/ziahamza/blend/db"
)

func ListenVertexEvents(wr http.ResponseWriter, rq *http.Request) {
	vars := mux.Vars(rq)
	vertex := &db.Vertex{Id: vars["vertex_id"]}

	err := db.GetVertex(vertex, false)

	if err != nil {
		ErrorHandler(wr, rq, APIResponse{Message: err.Error()})
		return
	}

	websocket.Handler(func(ws *websocket.Conn) {
		// TODO, for now just an echo server
		io.Copy(ws, ws)
	}).ServeHTTP(wr, rq)
}

func GetVertex(wr http.ResponseWriter, rq *http.Request) {
	vars := mux.Vars(rq)

	pkey := rq.FormValue("private_key")
	vertex := &db.Vertex{Id: vars["vertex_id"], PrivateKey: pkey}

	err := db.GetVertex(vertex, pkey != "")

	if err != nil {
		ErrorHandler(wr, rq, APIResponse{Message: err.Error()})
		return
	}

	if pkey != "" && pkey != vertex.PrivateKey {
		vertex.Private = ""
	}

	// never return the private key for now
	vertex.PrivateKey = ""
	DataHandler(wr, rq, APIResponse{
		Vertex: vertex,
	})
}

func CreateChildVertex(wr http.ResponseWriter, rq *http.Request) {
	var (
		vertex  db.Vertex
		cvertex db.Vertex
		edge    db.Edge
		err     error
	)

	vars := mux.Vars(rq)
	vertex.Id = vars["vertex_id"]

	vertex.PrivateKey = rq.FormValue("private_key")

	vbd := rq.FormValue("vertex")
	err = json.Unmarshal([]byte(vbd), &cvertex)
	if err != nil {
		ErrorHandler(wr, rq, APIResponse{Message: "Can't parse vertex metadata:" + vbd})
		return
	}

	ebd := rq.FormValue("edge")
	err = json.Unmarshal([]byte(ebd), &edge)
	if err != nil {
		ErrorHandler(wr, rq, APIResponse{Message: "Can't parse edge metadata:" + ebd})
		return
	}

	edge.From = vertex.Id

	edge.Family = "ownership"

	if vertex.PrivateKey != "" {
		if !db.ConfirmVertexKey(vertex.Id, vertex.PrivateKey) {
			ErrorHandler(wr, rq, APIResponse{Message: "Wrong private key " + vertex.PrivateKey + " specified for the source vertex " + vertex.Id})
			return
		}
	} else if edge.Name == "" {
		ErrorHandler(wr, rq, APIResponse{Message: "Either specify edge type and name precisely otherwise private key has to be specified"})
		return
	} else {
		if !db.ConfirmVertex(vertex.Id) {
			ErrorHandler(wr, rq, APIResponse{Message: fmt.Sprintf("Source vertex not found! %s", vertex.Id)})
			return
		}
	}

	fmt.Printf("Creating the child vertex %s under %s \n", cvertex.Id, vertex.Id)

	err = db.AddVertexChild(&cvertex, &edge)
	if err != nil {
		ErrorHandler(wr, rq, APIResponse{Message: err.Error()})
		return
	}

	fmt.Printf("Added a child vertex %s with edge %s successfully! \n", vertex.Id, edge.Name)

	// if private key the source not specified, hide private data for new child
	if vertex.PrivateKey == "" {
		cvertex.Private = ""
		cvertex.PrivateKey = ""
	}

	DataHandler(wr, rq, APIResponse{
		Vertex: &cvertex,
		Edge:   &edge,
	})
}

func CreateVertex(wr http.ResponseWriter, rq *http.Request) {
	vertex := db.Vertex{
		Name:       rq.FormValue("vertex_name"),
		Type:       rq.FormValue("vertex_type"),
		Public:     rq.FormValue("public_data"),
		Private:    rq.FormValue("private_data"),
		PrivateKey: "234234lkajsdflkajflakasdfasdflkj234234lk",
	}

	if vertex.Name == "" {
		ErrorHandler(wr, rq, APIResponse{Message: "Vertex name not specified ..."})
		return
	}

	if vertex.Type == "" {
		ErrorHandler(wr, rq, APIResponse{Message: "Vertex type not specified ..."})
		return
	}

	if vertex.Public == "" {
		ErrorHandler(wr, rq, APIResponse{Message: "Vertex public data left empty ..."})
		return
	}

	if vertex.Private == "" {
		ErrorHandler(wr, rq, APIResponse{Message: "Vertex private data left empty ... "})
	}

	// reset the ID if given
	vertex.Id = ""

	err := db.AddVertex(&vertex)

	if err != nil {
		ErrorHandler(wr, rq, APIResponse{Message: fmt.Sprintf("Cannot add a new vertex in the database (%s) %v: ", err.Error(), vertex)})
		return
	}

	fmt.Printf("Added a new vertex successfully: %s \n", vertex.Id)
	DataHandler(wr, rq, APIResponse{
		Vertex: &vertex,
	})
}
