package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func Handler() http.Handler {
	router := mux.NewRouter()
	router.Headers("Access-Control-Allow-Origin", "*")

	router.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
		writer.Write([]byte(
			`Blend: A distributed graph based filesystem for apps. Head over to /graph/help for api usage \n`))
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

func ErrorHandler(wr http.ResponseWriter, rq *http.Request, message string) {
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.WriteHeader(500)

	bd, err := json.Marshal(map[string]interface{}{
		"success": false,
		"message": message,
	})
	if err != nil {
		if err.Error() == "Unmatched column names/values" {
			// internal db error, exit for debugging ...
			panic(err.Error())
		}
		bd = []byte(`{ "success": false, "message": "` + err.Error() + `" }`)
	}
	wr.Write(bd)
	// fmt.Println("error:", message)
}

func DataHandler(wr http.ResponseWriter, rq *http.Request, data map[string]interface{}) {
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	bd, err := json.Marshal(data)

	if err != nil {
		ErrorHandler(wr, rq, err.Error())
		return
	}

	wr.WriteHeader(202)
	wr.Write([]byte(bd))
	// fmt.Println("sent the data:", string(bd))
}
