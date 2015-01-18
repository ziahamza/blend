package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ziahamza/blend/db"

	"github.com/gorilla/mux"
)

// only a subset of the following fields are send as the response
type APIResponse struct {
	Success bool       `json:"success"`
	Version string     `json:"graph-version"`
	Message string     `json:"message,omitempty"`
	Vertex  *db.Vertex `json:"vertex,omitempty"`
	Edge    *db.Edge   `json:"edge,omitempty"`
	Edges   *[]db.Edge `json:"edges,omitempty"`
	// TODO: add type to send an entire graph
}

func Handler() http.Handler {
	router := mux.NewRouter()
	router.Headers("Access-Control-Allow-Origin", "*")

	router.HandleFunc("/", func(wr http.ResponseWriter, rq *http.Request) {
		SendResponse(wr, APIResponse{
			Success: true,
			Message: `
				A distributed graph based filesystem for apps.
				Head over to /graph/help for api usage
			`,
		})
	})

	router.HandleFunc("/help", func(wr http.ResponseWriter, rq *http.Request) {
		http.ServeFile(wr, rq, "./static/API.md")
	})

	grouter := router.PathPrefix("/graph").Subrouter()

	// TODO: Hide the ability to create arbritary vertices as root nodes will be introduced soon.
	grouter.HandleFunc("/vertex", func(wr http.ResponseWriter, rq *http.Request) {
		var v db.Vertex
		vbd := rq.FormValue("vertex")
		err := json.Unmarshal([]byte(vbd), &v)
		if err != nil {
			SendResponse(wr, APIResponse{
				Success: false,
				Message: "Can't parse vertex:" + vbd,
			})

			return
		}

		SendResponse(wr, CreateVertex(v))
	}).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}", func(wr http.ResponseWriter, rq *http.Request) {
		vars := mux.Vars(rq)
		v := db.Vertex{Id: vars["vertex_id"], PrivateKey: rq.FormValue("private_key")}
		SendResponse(wr, GetVertex(v))
	}).Methods("GET")

	grouter.HandleFunc("/vertex/{vertex_id}", func(wr http.ResponseWriter, rq *http.Request) {
		vars := mux.Vars(rq)

		var (
			vertex      db.Vertex
			childVertex db.Vertex
			edge        db.Edge
		)

		vertex.Id = vars["vertex_id"]
		vertex.PrivateKey = rq.FormValue("private_key")

		vbd := rq.FormValue("vertex")
		err := json.Unmarshal([]byte(vbd), &childVertex)
		if err != nil {
			SendResponse(wr, APIResponse{
				Success: false,
				Message: "Can't parse vertex metadata:" + vbd,
			})

			return
		}

		ebd := rq.FormValue("edge")
		err = json.Unmarshal([]byte(ebd), &edge)
		if err != nil {
			SendResponse(wr, APIResponse{
				Success: false,
				Message: "Can't parse edge metadata:" + ebd,
			})
			return
		}

		SendResponse(wr, CreateChildVertex(vertex, childVertex, edge))
	}).Methods("POST")

	grouter.HandleFunc("/edge", func(wr http.ResponseWriter, rq *http.Request) {
		var e db.Edge

		// edge to add
		ebd := rq.FormValue("edge")

		err := json.Unmarshal([]byte(ebd), &e)
		if err != nil {
			SendResponse(wr, APIResponse{
				Success: false,
				Message: "Can't parse edge metadata:" + ebd,
			})
			return
		}

		if e.From == "" || e.To == "" {
			SendResponse(wr, APIResponse{
				Success: false,
				Message: fmt.Sprintf(
					"Source vertex or destination id not supplied. %s -> %s",
					e.From,
					e.To),
			})

			return
		}

		sourceVertex := db.Vertex{
			Id:         e.From,
			PrivateKey: rq.FormValue("private_key"),
		}

		destVertex := db.Vertex{Id: e.To}
		SendResponse(wr, CreateEdge(sourceVertex, destVertex, e))
	}).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}/edges", func(wr http.ResponseWriter, rq *http.Request) {
		vars := mux.Vars(rq)
		edge := db.Edge{
			From:   vars["vertex_id"],
			Family: rq.FormValue("edge_family"),
			Type:   rq.FormValue("edge_type"),
			Name:   rq.FormValue("edge_name"),
		}

		vertex := db.Vertex{
			Id:         vars["vertex_id"],
			PrivateKey: rq.FormValue("private_key"),
		}

		/*
			if edge.Family == "" {
				edge.Family = "public"
			}
		*/

		SendResponse(wr, GetEdges(vertex, edge))
	}).Methods("GET")

	// grouter.HandleFunc("/vertex/{vertex_id}/events", ListenVertexEvents).Methods("GET")

	return router
}

func SendResponse(wr http.ResponseWriter, resp APIResponse) {
	resp.Version = "0.0.1"

	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Access-Control-Allow-Origin", "*")

	bd, err := json.Marshal(resp)

	if err != nil {
		// json marshalling somehow failed, fallback to manual error response
		wr.WriteHeader(400)
		bd = []byte(`
			{
				"success": false,
				"message": "` + resp.Message + `",
				"graph-version": ` + resp.Version + `
			}
		`)
	} else if resp.Success {
		wr.WriteHeader(202)
	} else {
		wr.WriteHeader(400)
	}

	wr.Write([]byte(bd))
}
