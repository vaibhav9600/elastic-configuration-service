package handlers

import (
	"encoding/json"
	"net/http"

	"elastic-search-config-service/models"
	"elastic-search-config-service/services"

	"github.com/gorilla/mux"
)

func PostDocuments(esClient *services.ElasticsearchClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		indexName := vars["index_name"]

		var documents []models.Document
		err := json.NewDecoder(r.Body).Decode(&documents)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = esClient.IndexDocuments(indexName, documents)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"message": "Documents indexed successfully"})
	}
}
