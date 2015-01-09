package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/ziahamza/blend/db"
)

func GetEdges(wr http.ResponseWriter, rq *http.Request) {
	vars := mux.Vars(rq)

	edge := db.Edge{
		From:   vars["vertex_id"],
		Family: rq.FormValue("edge_family"),
		Type:   rq.FormValue("edge_type"),
		Name:   rq.FormValue("edge_name"),
	}

	vkey := rq.FormValue("private_key")

	if edge.Family == "" {
		edge.Family = "public"
	}

	// confirm vertex data
	if vkey == "" {
		if !db.ConfirmVertex(edge.From) {
			ErrorHandler(wr, rq, "Vertex with the specific id not found:"+edge.From)
			return
		}
	} else {
		if !db.ConfirmVertexKey(edge.From, vkey) {
			ErrorHandler(wr, rq, "Wrong private key "+vkey+" supplied for source vertex "+edge.From)
			return
		}
	}

	if edge.Family != "public" {
		if edge.Family != "ownership" && edge.Family != "private" && edge.Family != "event" {
			ErrorHandler(wr, rq, "Edge family not supported!")
			return
		}
		// its a well specified edge, no need for private key
		// even if its ownership or private edges
		// otherwise the private key needs to be confirmed
		if (edge.Type == "" || edge.Name == "") && vkey == "" {
			ErrorHandler(wr, rq, "Either private_key needs to be supplied or the edge type and name have to be known beforehand")
			return
		}
	}

	edges, err := db.GetEdges(edge)

	if err != nil {
		ErrorHandler(wr, rq, err.Error())
		return
	}

	// remove edge data as private key was not supplied
	if edge.Family != "public" && vkey == "" {
		for i := range edges {
			edges[i].Data = ""
		}
	}

	DataHandler(wr, rq, map[string]interface{}{
		"success": true,
		"edges":   edges,
	})
}

func CreateEdge(wr http.ResponseWriter, rq *http.Request) {
	edge := &db.Edge{}

	// source private key
	vkey := rq.FormValue("private_key")

	// edge to add
	ebd := rq.FormValue("edge")

	err := json.Unmarshal([]byte(ebd), edge)
	if err != nil {
		ErrorHandler(wr, rq, "Can't parse edge metadata:"+ebd)
		return
	}

	if edge.Family != "private" && edge.Family != "public" {
		ErrorHandler(wr, rq, "Invalid edge family. Only the following edge families are supported: private and public. ")
		return
	}

	if edge.From == "" || edge.To == "" {
		ErrorHandler(wr, rq, fmt.Sprintf("Source vertex or destination id not supplied. %s -> %s", edge.From, edge.To))
		return
	}

	if vkey != "" {
		if !db.ConfirmVertexKey(edge.From, vkey) {
			ErrorHandler(wr, rq, "Wrong source private key "+vkey+"supplied for vertex "+edge.From)
			return
		}
	} else {
		if !db.ConfirmVertex(edge.From) {
			ErrorHandler(wr, rq, "Source vertex not found.")
			return
		}
	}

	if !db.ConfirmVertex(edge.To) {
		ErrorHandler(wr, rq, fmt.Sprintf("Destination vertex not found %s", edge.To))
		return
	}

	if edge.From == edge.To {
		ErrorHandler(wr, rq, "Destination and source vertex are the same.")
		return
	}

	if edge.Type == "" && edge.Name == "" {
		ErrorHandler(wr, rq, "Both edge type and name missing.")
		return
	}

	if edge.Family == "private" && edge.Name != "" && vkey == "" {
		ErrorHandler(wr, rq, "Creating unique private edges requirs a private key")
		return
	}

	err = db.AddEdge(edge)
	if err != nil {
		ErrorHandler(wr, rq, err.Error())
		return
	}

	fmt.Printf("Added a new edge successfully: %s -> %s (%s) \n", edge.From, edge.To, edge.Name)

	DataHandler(wr, rq, map[string]interface{}{
		"success": true,
		"edge":    edge,
	})
}
