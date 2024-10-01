package router

import (
	"elastic-search-config-service/handlers"
	"elastic-search-config-service/services"

	"github.com/gorilla/mux"
)

func NewRouter(esClient *services.ElasticsearchClient) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/index", handlers.PostIndex(esClient)).Methods("POST")
	r.HandleFunc("/index/settings", handlers.PostIndexSettings(esClient)).Methods("POST")
	r.HandleFunc("/{index_name}/documents", handlers.PostDocuments(esClient)).Methods("POST")
	r.HandleFunc("/{index_name}/attributes", handlers.GetIndexAttributesHandler(esClient)).Methods("GET")

	return r
}
