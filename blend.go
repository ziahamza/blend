package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/ziahamza/blend/db"
	"github.com/ziahamza/blend/handlers"
)

func InitSchema() error {
	rootVertex := &db.Vertex{
		Id:         "root",
		Name:       "root",
		Type:       "root",
		PrivateKey: "root",
	}

	err := db.AddVertex(rootVertex)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	backend := flag.String("backend", "memory", "Storage backend for storing graph vertices. Memory for now. Cassandra and local storage options comming soon")
	uri := flag.String("uri", "", "URI for the storage backend. IF the storage backend is cassandra then the URI will be the IP of a cassandra node. If the backend is local storage then the URI will be the path to the database file. Leave the URI to be blank for in memory storage backend.")
	listen := flag.String("port", ":8080", "Port and host for api server to listen on")
	drop := flag.Bool("drop", false, "Recreate the cassandra schema")

	flag.Parse()

	var err error

	if *backend != "memory" {
		log.Fatal("Specific backend not yet supported, use the memory default backend")
	}

	err = db.Init(*uri, &db.MemoryStorage{})

	if err != nil {
		fmt.Printf("Cannot connect to the storage backend on %s. Try passing a different URI for backend (%s) \n", *uri, err.Error())
		return
	}

	defer db.Close()

	err = InitSchema()
	if err != nil {
		log.Fatal(err)
	}

	if *drop {
		err = db.Drop()
		if err != nil {
			log.Fatal("Failed to Drop DB tables and create a new schema: ", err)
		}

		fmt.Println("Recreated Blend Schema and Root Vertices successfully!")
	}

	router := mux.NewRouter()
	router.Headers("Access-Control-Allow-Origin", "*")

	router.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
		writer.Write([]byte("Blend: A distributed graph based filesystem for apps. Head over to /graph/help for api usage \n"))
	})

	grouter := router.PathPrefix("/graph").Subrouter()

	grouter.HandleFunc("/help", func(wr http.ResponseWriter, rq *http.Request) {
		http.ServeFile(wr, rq, "./static/API.md")
	})

	grouter.HandleFunc("/edge", handlers.CreateEdge).Methods("POST")

	// TODO: Hide the ability to create arbritary vertices as root nodes will be introduced soon.
	grouter.HandleFunc("/vertex", handlers.CreateVertex).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}", handlers.GetVertex).Methods("GET")

	grouter.HandleFunc("/vertex/{vertex_id}", handlers.CreateChildVertex).Methods("POST")

	grouter.HandleFunc("/vertex/{vertex_id}/edges", handlers.GetEdges).Methods("GET")

	grouter.HandleFunc("/vertex/{vertex_id}/events", handlers.ListenVertexEvents).Methods("GET")

	http.Handle("/", router)
	fmt.Printf("Blend Graph listening on host %s\n", *listen)
	err = http.ListenAndServe(*listen, nil)

	fmt.Printf("Server crash: %s\n", err.Error())

	log.Fatal(err)
}
