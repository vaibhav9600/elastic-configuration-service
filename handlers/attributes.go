package handlers

import (
	"elastic-search-config-service/services" // Replace with your actual import path for Elasticsearch client
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func GetIndexAttributesHandler(esClient *services.ElasticsearchClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		indexName := vars["index_name"]

		attributes, err := esClient.GetIndexAttributes(indexName)
		if err != nil {
			http.Error(w, "Error fetching index attributes: "+err.Error(), http.StatusInternalServerError)
			return
		}

		jsonResponse, err := json.Marshal(attributes)
		if err != nil {
			http.Error(w, "Error converting response to JSON: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
	}
}
