package db

import (
	"errors"
	"fmt"

	"github.com/ziahamza/blend"

	"github.com/gocql/gocql"
)

type CassandraStorage struct {
	session *gocql.Session
}

func (backend *CassandraStorage) Init(cqlurl string) error {
	var err error

	if len(cqlurl) == 0 {
		cqlurl = "localhost"
	}

	fmt.Printf("using cassandra cluster at IP=%s\n", cqlurl)

	// connect to the cluster
	cluster := gocql.NewCluster(cqlurl)
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	err = session.Query(`
		CREATE KEYSPACE graph WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : 2
		}
	`).Consistency(gocql.All).Exec()

	session.Close()

	refreshDb := false
	if err != nil {
		fmt.Printf("graph keyspace already created: %s\n", err.Error())
	} else {
		fmt.Printf("Keyspace 'graph' created. \n")

		// newly created keyspace ... drop the database
		refreshDb = true
	}

	cluster.Keyspace = "graph"
	backend.session, err = cluster.CreateSession()

	if err != nil && refreshDb {
		backend.Drop()
	}

	return err
}

func (backend *CassandraStorage) Close() {
	backend.session.Close()
}

func (backend *CassandraStorage) Drop() error {
	// drop vertices table for development
	err := backend.session.Query("DROP TABLE vertices;").Exec()
	if err != nil {
		fmt.Println("Cannot drop vertices table: ", err.Error())
	}

	// drop vertices table for development
	err = backend.session.Query("DROP TABLE edges;").Exec()
	if err != nil {
		fmt.Println("Cannot drop edges table: ", err.Error())
	}

	// initialize vertices table
	err = backend.session.Query(
		`CREATE TABLE vertices (
			edge_family varchar,
			edge_type varchar,
			edge_name varchar,
			from_vertex_id varchar,

			public_data varchar static,
			private_data varchar static,
			private_key varchar static,

			vertex_type varchar static,
			vertex_name varchar static,

			last_changed timeuuid static,

			vertex_id varchar,
			PRIMARY KEY (vertex_id, edge_family, edge_type, edge_name, from_vertex_id)
		);`,
	).Exec()

	if err != nil {
		fmt.Println("Canoot create a new table called vertex ... ", err.Error())
		return err
	}

	// initialize edges table
	err = backend.session.Query(
		`CREATE TABLE edges (
			edge_family varchar,
			edge_type varchar,
			edge_name varchar,
			edge_data varchar,
			from_vertex_id varchar,
			to_vertex_id varchar,

			last_changed timeuuid static,

			PRIMARY KEY (from_vertex_id, edge_family, edge_type, edge_name, to_vertex_id)
		);`,
	).Exec()

	if err != nil {
		fmt.Println("Canoot create a new table called edge ... ", err.Error())
		return err
	}

	return nil
}

func (backend *CassandraStorage) UpdateVertex(vertex *blend.Vertex) error {
	err := backend.session.Query(
		`UPDATE vertices SET vertex_name = ?, vertex_type = ?, public_data = ?, private_data = ?
		WHERE vertex_id = ? `,
		vertex.Name, vertex.Type, vertex.Public, &vertex.Private, vertex.Id,
	).Consistency(gocql.Two).Exec()

	if err != nil {
		return err
	}

	err = backend.session.Query(
		`SELECT private_key FROM vertices WHERE vertex_id = ? LIMIT 1;`, vertex.Id,
	).Consistency(gocql.One).Scan(&vertex.PrivateKey)

	return err
}

func (backend *CassandraStorage) GetVertex(vertex *blend.Vertex) error {
	vkey := vertex.PrivateKey
	if vkey != "" {
		err := backend.session.Query(
			`SELECT vertex_name, vertex_type, public_data, private_data, private_key
			FROM vertices WHERE vertex_id = ? LIMIT 1;`,
			vertex.Id,
		).Consistency(gocql.One).Scan(
			&vertex.Name, &vertex.Type,
			&vertex.Public, &vertex.Private, &vertex.PrivateKey,
		)

		if err != nil {
			return err
		}

		if vkey != vertex.PrivateKey {
			return errors.New("Private Key Not Supplied")
		}
	}

	return backend.session.Query(
		`SELECT vertex_name, vertex_type, public_data
		FROM vertices WHERE vertex_id = ? LIMIT 1;`,
		vertex.Id,
	).Consistency(gocql.One).Scan(
		&vertex.Name, &vertex.Type,
		&vertex.Public,
	)
}

func (backend *CassandraStorage) GetEdges(edge blend.Edge) ([]blend.Edge, error) {
	edges := []blend.Edge{}

	var iter *gocql.Iter
	if edge.Type == "" {
		// get all edges by a specific family
		iter = backend.session.Query(
			`SELECT edge_name, edge_type, edge_family, to_vertex_id, edge_data
			FROM edges WHERE from_vertex_id = ? AND edge_family = ?;`,
			edge.From, edge.Family,
		).Consistency(gocql.One).Iter()
	} else if edge.Name == "" {
		// get all edges by a specific family and a specific type
		iter = backend.session.Query(
			`SELECT edge_name, edge_type, edge_family, to_vertex_id, edge_data
			FROM edges WHERE from_vertex_id = ? AND edge_family = ? AND edge_type = ?;`,
			edge.From, edge.Family, edge.Type,
		).Consistency(gocql.One).Iter()
	} else {
		// get all edges by a specific family, type and name
		iter = backend.session.Query(
			`SELECT edge_name, edge_type, edge_family, to_vertex_id, edge_data
			FROM edges WHERE from_vertex_id = ? AND edge_family = ? AND edge_type = ? AND edge_name = ?;`,
			edge.From, edge.Family, edge.Type, edge.Name,
		).Consistency(gocql.One).Iter()
	}

	for iter.Scan(&edge.Name, &edge.Type, &edge.Family, &edge.To, &edge.Data) {
		edges = append(edges, edge)
	}

	return edges, nil
}

func (backend *CassandraStorage) AddEdge(edge *blend.Edge) error {
	err := backend.session.Query(
		`SELECT to_vertex_id
		FROM edges WHERE from_vertex_id = ? AND edge_family = ? AND edge_type = ? AND edge_name = ?;`,
		edge.From, edge.Family, edge.Type, edge.Name,
	).Consistency(gocql.One).Scan(&edge.To)

	// if the edge exists, then just return as edge is filled up with new data
	if err == nil {
		fmt.Println("edge found already, returning the old edge: ", edge.Family, ":", edge.Type, ":", edge.Name)
		return nil
	}

	// Add the edge in the source vertex row
	err = backend.session.Query(
		`INSERT INTO edges (from_vertex_id, to_vertex_id, edge_family, edge_type, edge_name, edge_data)
		VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS;`,
		edge.From, edge.To, edge.Family, edge.Type, edge.Name, edge.Data,
	).Consistency(gocql.Two).Exec()

	if err != nil {
		return err
	}

	// ownership edges are two way, for private key change and propogating events
	return backend.session.Query(
		`INSERT INTO vertices(vertex_id, from_vertex_id, edge_family, edge_type, edge_name)
		VALUES (?, ?, ?, ?, ?) IF NOT EXISTS;`,
		edge.To, edge.From, edge.Family, edge.Type, edge.Name,
	).Consistency(gocql.Two).Exec()

	return nil
}

func (backend *CassandraStorage) AddVertex(vertex *blend.Vertex) error {
	err := backend.session.Query(
		`INSERT INTO vertices (
			vertex_id, vertex_name, vertex_type, public_data, private_data, private_key
		) VALUES (?, ?, ?, ?, ?, ?);`,
		vertex.Id, vertex.Name, vertex.Type, vertex.Public, vertex.Private, vertex.PrivateKey,
	).Consistency(gocql.Two).Exec()

	return err
}

func (backend *CassandraStorage) DeleteVertex(vertex *blend.Vertex) error {
	return backend.session.Query(
		`BEGIN BATCH
			DELETE FROM vertices WHERE vertex_id = ?
			DELETE FROM edges WHERE from_vertex_id = ?
		APPLY BATCH;`,
		vertex.Id, vertex.Id,
	).Consistency(gocql.Two).Exec()
}

func (backend *CassandraStorage) DeleteVertexTree(vertices []*blend.Vertex) error {
	if len(vertices) == 0 {
		return nil
	}

	vertex := vertices[0]
	vertices = vertices[1:]

	backEdges, err := backend.GetEdges(blend.Edge{From: vertex.Id, Family: "ownership"})

	if err != nil {
		return err
	}

	// Breadth first deletion
	for _, edge := range backEdges {
		vertices = append(vertices, &blend.Vertex{Id: edge.To})
	}
	err = backend.DeleteVertexTree(vertices)

	if err != nil {
		return err
	}

	return backend.DeleteVertex(vertex)
}
