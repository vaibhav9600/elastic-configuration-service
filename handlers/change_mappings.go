package handlers

import (
	"elastic-search-config-service/models"
	"elastic-search-config-service/services"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// TODO: add dynamic update support using scripts
func ChangeMappings(esClient *services.ElasticsearchClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		indexName := vars["index_name"]

		var settings models.IndexSettings
		err := json.NewDecoder(r.Body).Decode(&settings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// TODO: add verification for fields names here whether they are present in mappings
		// we can call GetIndexAttributes from within services
		// give implementation below

		// validating if some field info passed or not else return
		if len(settings.SearchableAttributes) == 0 && len(settings.FacetAttributes) == 0 {
			http.Error(w, "non empty index settings not allowed", http.StatusBadRequest)
		}
		ind := models.GetIndexInfo(models.IndexName{Index: indexName})
		err = esClient.ChangeMappings(ind, settings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"message": "Mappings for index changed successfully"})
	}
}
