package handlers

import (
	"encoding/json"
	"net/http"
)

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
