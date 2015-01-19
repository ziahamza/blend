package blend

import (
	"time"
)

type Vertex struct {
	Id          string    `json:"vertex_id"`
	LastChanged time.Time `json:"last_changed"`
	Name        string    `json:"vertex_name"`
	Type        string    `json:"vertex_type"`
	Private     string    `json:"private_data,omitempty"`
	PrivateKey  string    `json:"private_key,omitempty"`
	Public      string    `json:"public_data"`
}

type Event struct {
	Source  string    `json:"verted_id"`
	Type    string    `json:"event_type"`
	Created time.Time `json:"event_time"`
}

// EDGE types: ownership, public, private and event
type Edge struct {
	LastChanged string `json:"last_changed"`
	Family      string `json:"edge_family"`
	Type        string `json:"edge_type"`
	Name        string `json:"edge_name"`
	From        string `json:"vertex_from"`
	To          string `json:"vertex_to"`
	Data        string `json:"edge_data"`
}
type APIRequest struct {
	Method      string `json:"method,omitempty"`
	Edge        Edge   `json:"edge,omitempty"`
	Vertex      Vertex `json:"vertex,omitempty"`
	ChildVertex Vertex `json:"child_vertex,omitempty"`
}

// only a subset of the following fields are send as the response
type APIResponse struct {
	Success bool    `json:"success"`
	Version string  `json:"graph-version"`
	Message string  `json:"message,omitempty"`
	Vertex  *Vertex `json:"vertex,omitempty"`
	Edge    *Edge   `json:"edge,omitempty"`
	Edges   *[]Edge `json:"edges,omitempty"`
	// TODO: add type to send an entire graph
}
