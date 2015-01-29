package api

import (
	"fmt"

	"github.com/ziahamza/blend"

	"github.com/ziahamza/blend/db"
)

/*
func ListenVertexEvents(wr http.ResponseWriter, rq *http.Request) {
	vars := mux.Vars(rq)
	vertex := &blend.Vertex{Id: vars["vertex_id"]}

	err := db.GetVertex(vertex, false)

	if err != nil {
		ErrorHandler(wr, rq, blend.APIResponse{Message: err.Error()})
		return
	}

	websocket.Handler(func(ws *websocket.Conn) {
		// TODO, for now just an echo server
		io.Copy(ws, ws)
	}).ServeHTTP(wr, rq)
}
*/

func GetVertex(v blend.Vertex) blend.APIResponse {
	if v.Id == "" {
		return blend.APIResponse{Success: false, Message: "Vertex Id not supplied"}
	}

	err := db.GetVertex(&v)

	if err != nil {
		return blend.APIResponse{Success: false, Message: err.Error()}
	}

	return blend.APIResponse{Success: true, Vertex: &v}
}

func CreateChildVertex(vertex blend.Vertex, childVertex blend.Vertex, e blend.Edge) blend.APIResponse {
	if vertex.Id == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Vertex details empty",
		}
	}

	e.From = vertex.Id
	e.Family = "ownership"

	err := db.GetVertex(&vertex)
	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: err.Error(),
		}
	}

	if vertex.PrivateKey == "" && (e.Name == "" || e.Type == "") {
		return blend.APIResponse{
			Success: false,
			Message: "Edge type and name cannot be empty if private key is not supplied",
		}
	}

	fmt.Printf("Creating the child vertex under %s \n", vertex.Id)

	err = db.CreateChildVertex(&vertex, &childVertex, e)
	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: err.Error(),
		}
	}

	fmt.Printf("Added a child vertex %s with edge %s successfully! \n", childVertex.Id, e.Name)

	// if private key the source not specified, hide private data for new child
	if vertex.PrivateKey == "" {
		childVertex.Private = ""
		childVertex.PrivateKey = ""
	}

	return blend.APIResponse{
		Success: true,
		Vertex:  &childVertex,
		Edge:    &e,
	}
}

func CreateVertex(v blend.Vertex) blend.APIResponse {
	if v.Name == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Vertex name not specified ...",
		}
	}

	if v.Type == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Vertex type not specified ...",
		}
	}

	if v.Public == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Vertex type not specified ...",
		}
	}

	if v.Private == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Vertex private data left empty ... ",
		}
	}

	err := db.CreateVertex(&v)

	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: fmt.Sprintf("Cannot add a new vertex in the database (%s): ", err.Error()),
		}
	}

	fmt.Printf("Added a new vertex successfully: %s \n", v.Id)
	return blend.APIResponse{
		Success: true,
		Vertex:  &v,
	}
}
