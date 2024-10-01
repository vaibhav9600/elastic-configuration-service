package handlers

import (
	"encoding/json"
	"net/http"

	"elastic-search-config-service/services"
)

func PostIndexSettings(esClient *services.ElasticsearchClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var settings map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&settings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = esClient.UpdateIndexSettings(settings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Index settings updated successfully"})
	}
}
