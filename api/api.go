package api

import (
	"encoding/json"
	"net/http"

	"github.com/ziahamza/blend/db"

	"github.com/gorilla/mux"
)

// only a subset of the following fields are send everytime a request comes in
type APIResponse struct {
	Success bool       `json:"success"`
	Version string     `json:"graph-version"`
	Message string     `json:"message,omitempty"`
	Vertex  *db.Vertex `json:"vertex,omitempty"`
	Edge    *db.Edge   `json:"edge,omitempty"`
	Edges   *[]db.Edge `json:"edges,omitempty"`
}

func Handler() http.Handler {
	router := mux.NewRouter()
	router.Headers("Access-Control-Allow-Origin", "*")

	router.HandleFunc("/", func(wr http.ResponseWriter, rq *http.Request) {
		DataHandler(wr, rq, APIResponse{
			Message: "A distributed graph based filesystem for apps. Head over to /graph/help for api usage",
		})
	})

	router.HandleFunc("/help", func(wr http.ResponseWriter, rq *http.Request) {
		http.ServeFile(wr, rq, "./static/API.md")
	})

	grouter := router.PathPrefix("/graph").Subrouter()

	grouter.HandleFunc("/edge", CreateEdge).Methods("POST")

	// TODO: Hide the ability to create arbritary vertices as root nodes will be introduced soon.
	grouter.HandleFunc("/vertex", CreateVertex).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}", GetVertex).Methods("GET")

	grouter.HandleFunc("/vertex/{vertex_id}", CreateChildVertex).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}/edges", GetEdges).Methods("GET")

	grouter.HandleFunc("/vertex/{vertex_id}/events", ListenVertexEvents).Methods("GET")

	return router
}

func ErrorHandler(wr http.ResponseWriter, rq *http.Request, resp APIResponse) {
	resp.Version = "0.0.1"

	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.WriteHeader(500)

	// not using json encoding, in case it fails
	bd := []byte(`
		{
			"success": false,
			"message": "` + resp.Message + `",
			"graph-version": ` + resp.Version + `
		}
	`)
	wr.Write(bd)

	// fmt.Println("error:", message)
}

func DataHandler(wr http.ResponseWriter, rq *http.Request, resp APIResponse) {
	resp.Success = true
	resp.Version = "0.0.1"

	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	bd, err := json.Marshal(resp)

	if err != nil {
		ErrorHandler(wr, rq, err.Error())
		return
	}

	wr.WriteHeader(202)
	wr.Write([]byte(bd))
	// fmt.Println("sent the data:", string(bd))
}
