package handlers

import (
	"elastic-search-config-service/models"
	"elastic-search-config-service/services"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func Search(esClient *services.ElasticsearchClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		indexName := vars["index_name"]

		var data models.SearchReq
		err := json.NewDecoder(r.Body).Decode(&data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data.IndexName = indexName
		// apply validation on index names here
		res, err := esClient.Search(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse, err := json.Marshal(res)
		if err != nil {
			http.Error(w, "Error converting response to JSON: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
	}
}
