package api

import (
	"fmt"

	"github.com/ziahamza/blend"
	"github.com/ziahamza/blend/db"
)

func GetEdges(v blend.Vertex, e blend.Edge) blend.APIResponse {
	if v.Id == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Vertex ID not supplied",
		}
	}

	switch e.Family {
	case "":
		return blend.APIResponse{
			Success: false,
			Message: "Edge family not supplied",
		}
	case "public", "private", "ownership":
		// do nothing
	default:
		return blend.APIResponse{
			Success: false,
			Message: "Unknown edge family given",
		}

	}

	err := db.GetVertex(&v)
	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: err.Error(),
		}
	}

	if e.Family != "public" {

		// its a well specified edge, no need for private key
		// even if its ownership or private edges
		// otherwise the private key needs to be confirmed
		if (e.Type == "" || e.Name == "") && v.PrivateKey == "" {
			return blend.APIResponse{
				Success: false,
				Message: `Either private_key needs to be supplied or the
					edge type and name have to be known beforehand`,
			}
		}
	}

	edges, err := db.GetEdges(v, e)

	if err != nil {
		return blend.APIResponse{Success: false, Message: err.Error()}
	}

	// remove edge data as private key was not supplied
	if e.Family != "public" && v.PrivateKey == "" {
		for i := range edges {
			edges[i].Data = ""
		}
	}

	return blend.APIResponse{
		Success: true,
		Edges:   &edges,
	}
}

func CreateEdge(sourceVertex, destVertex blend.Vertex, e blend.Edge) blend.APIResponse {
	var err error

	switch e.Family {
	case "":
		return blend.APIResponse{Success: false, Message: "Edge Family not given"}
	case "private", "public":
		// fall through
	default:
		return blend.APIResponse{Success: false, Message: "Unknown edge famliy supplied"}
	}

	if sourceVertex.Id == "" || destVertex.Id == "" {
		return blend.APIResponse{
			Success: false,
			Message: fmt.Sprintf(
				"Source vertex or destination id not supplied. %s -> %s",
				e.From,
				e.To),
		}

	}

	if sourceVertex.Id == destVertex.Id {
		return blend.APIResponse{
			Success: false,
			Message: "Destination and source vertex are the same.",
		}
	}

	e.From = sourceVertex.Id
	e.To = destVertex.Id

	err = db.GetVertex(&sourceVertex)
	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: err.Error(),
		}
	}

	err = db.GetVertex(&destVertex)
	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: err.Error(),
		}
	}

	if e.Type == "" && e.Name == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Both edge type and name missing.",
		}
	}

	if e.Family == "private" && e.Name != "" && sourceVertex.PrivateKey == "" {
		return blend.APIResponse{
			Success: false,
			Message: "Creating unique private edges requirs a private key",
		}
	}

	err = db.CreateEdge(sourceVertex, &e)
	if err != nil {
		return blend.APIResponse{
			Success: false,
			Message: err.Error(),
		}
	}

	fmt.Printf("Added a new edge successfully: %s -> %s (%s) \n", e.From, e.To, e.Name)

	return blend.APIResponse{
		Success: true,
		Edge:    &e,
	}
}
