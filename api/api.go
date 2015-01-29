package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/ziahamza/blend"
)

func HandleRequest(req blend.APIRequest) blend.APIResponse {
	switch req.Method {
	case "/":
		return GetInfo()
	case "/vertex/get":
		return GetVertex(req.Vertex)
	case "/vertex/create":
		return CreateVertex(req.Vertex)
	case "/vertex/createChild":
		return CreateChildVertex(req.Vertex, req.ChildVertex, req.Edge)

	case "/edge/get":
		return GetEdges(req.Vertex, req.Edge)
	case "/edge/create":
		return CreateEdge(req.Vertex, req.ChildVertex, req.Edge)
	default:
		return blend.APIResponse{Success: false, Message: "Unknown request method"}
	}
}

func GetInfo() blend.APIResponse {
	return blend.APIResponse{
		Success: true,
		Message: `
			A distributed graph based filesystem for apps.
			Head over to /graph/help for api usage
		`,
	}
}

func Handler() http.Handler {
	router := mux.NewRouter()
	router.Headers("Access-Control-Allow-Origin", "*")

	router.HandleFunc("/", func(wr http.ResponseWriter, rq *http.Request) {
		SendResponse(wr, GetInfo())
	})

	router.HandleFunc("/help", func(wr http.ResponseWriter, rq *http.Request) {
		http.ServeFile(wr, rq, "./static/API.md")
	})

	grouter := router.PathPrefix("/graph").Subrouter()

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	grouter.HandleFunc("/rpc", func(wr http.ResponseWriter, rq *http.Request) {
		conn, err := upgrader.Upgrade(wr, rq, nil)
		if err != nil {
			fmt.Printf("Error with rpc: %s %v \n", err.Error())
			return
		}

		for {
			var (
				req  blend.APIRequest
				resp blend.APIResponse
			)
			err = conn.ReadJSON(&req)

			if err != nil {
				resp = blend.APIResponse{
					Success: false,
					Message: "Error Parsing api request" + err.Error(),
				}
			} else {
				resp = HandleRequest(req)
			}

			err = conn.WriteJSON(&resp)
			if err != nil {
				break
			}
		}
	})

	// TODO: Hide the ability to create arbritary vertices as root nodes will be introduced soon.
	grouter.HandleFunc("/vertex", func(wr http.ResponseWriter, rq *http.Request) {
		var v blend.Vertex
		vbd := rq.FormValue("vertex")
		err := json.Unmarshal([]byte(vbd), &v)
		if err != nil {
			SendResponse(wr, blend.APIResponse{
				Success: false,
				Message: "Can't parse vertex:" + vbd,
			})

			return
		}

		SendResponse(wr, CreateVertex(v))
	}).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}", func(wr http.ResponseWriter, rq *http.Request) {
		vars := mux.Vars(rq)
		v := blend.Vertex{Id: vars["vertex_id"], PrivateKey: rq.FormValue("private_key")}
		SendResponse(wr, GetVertex(v))
	}).Methods("GET")

	grouter.HandleFunc("/vertex/{vertex_id}", func(wr http.ResponseWriter, rq *http.Request) {
		vars := mux.Vars(rq)

		var (
			vertex      blend.Vertex
			childVertex blend.Vertex
			edge        blend.Edge
		)

		vertex.Id = vars["vertex_id"]
		vertex.PrivateKey = rq.FormValue("private_key")

		vbd := rq.FormValue("vertex")
		err := json.Unmarshal([]byte(vbd), &childVertex)
		if err != nil {
			SendResponse(wr, blend.APIResponse{
				Success: false,
				Message: "Can't parse vertex metadata:" + vbd,
			})

			return
		}

		ebd := rq.FormValue("edge")
		err = json.Unmarshal([]byte(ebd), &edge)
		if err != nil {
			SendResponse(wr, blend.APIResponse{
				Success: false,
				Message: "Can't parse edge metadata:" + ebd,
			})
			return
		}

		SendResponse(wr, CreateChildVertex(vertex, childVertex, edge))
	}).Methods("POST")

	grouter.HandleFunc("/edge", func(wr http.ResponseWriter, rq *http.Request) {
		var e blend.Edge

		// edge to add
		ebd := rq.FormValue("edge")

		err := json.Unmarshal([]byte(ebd), &e)
		if err != nil {
			SendResponse(wr, blend.APIResponse{
				Success: false,
				Message: "Can't parse edge metadata:" + ebd,
			})
			return
		}

		sourceVertex := blend.Vertex{
			Id:         e.From,
			PrivateKey: rq.FormValue("private_key"),
		}

		destVertex := blend.Vertex{Id: e.To}
		SendResponse(wr, CreateEdge(sourceVertex, destVertex, e))
	}).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}/edges", func(wr http.ResponseWriter, rq *http.Request) {
		vars := mux.Vars(rq)
		edge := blend.Edge{
			From:   vars["vertex_id"],
			Family: rq.FormValue("edge_family"),
			Type:   rq.FormValue("edge_type"),
			Name:   rq.FormValue("edge_name"),
		}

		vertex := blend.Vertex{
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

func SendResponse(wr http.ResponseWriter, resp blend.APIResponse) {
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
