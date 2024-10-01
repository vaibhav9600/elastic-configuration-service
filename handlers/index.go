package handlers

import (
	"encoding/json"
	"net/http"

	"elastic-search-config-service/models"
	"elastic-search-config-service/services"
)

func PostIndex(esClient *services.ElasticsearchClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var data models.IndexName
		err := json.NewDecoder(r.Body).Decode(&data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ind := models.GetIndexInfo(data)
		// apply validation on index names here
		err = esClient.CreateIndexAndAliases(ind)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"message": "Index created successfully"})
	}
}
