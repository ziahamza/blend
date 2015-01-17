package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/ziahamza/blend/api"
	"github.com/ziahamza/blend/db"
	"github.com/ziahamza/blend/events"
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
	backend := flag.String("backend", "local",
		`Storage backend for the graph. Possible values include local, cassandra`)

	uri := flag.String("uri", path.Join(os.TempDir(), "blend.db"),
		`URI for the storage backend. IF the storage
backend is cassandra then the URI will be the IP of a cassandra node.
If the backend is local storage then the URI will be the path to the
database file.`)

	listen := flag.String("port", ":8080", "Port and host for api server to listen on")
	drop := flag.Bool("drop", false, "reset the backend storage schema")

	flag.Parse()

	var err error

	if *backend == "local" {
		err = db.Init(*uri, &db.BoltStorage{})
	} else if *backend == "cassandra" {
		err = db.Init(*uri, &db.CassandraStorage{})
	} else {
		log.Fatal("Backend not supported!")
	}

	if err != nil {
		fmt.Printf("Cannot connect to the storage backend on %s \n", *uri)
		fmt.Printf("Try passing a different URI for backend (%s) \n", err.Error())
		return
	}

	defer db.Close()

	err = InitSchema()
	if err != nil {
		log.Fatal(err)
	}

	events.Init()

	if *drop {
		err = db.Drop()
		if err != nil {
			log.Fatal("Failed to Drop DB tables and create a new schema: ", err)
		}

		fmt.Println("Recreated Blend Schema and Root Vertices successfully!")
	}

	http.Handle("/", api.Handler())

	fmt.Printf("Blend Graph listening on host %s\n", *listen)
	err = http.ListenAndServe(*listen, nil)

	fmt.Printf("Server crash: %s\n", err.Error())

	log.Fatal(err)
}
