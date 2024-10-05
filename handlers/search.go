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
		_, err = esClient.Search(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusFound)
		json.NewEncoder(w).Encode(map[string]string{"message": "Search Request Successful"})
	}
}
