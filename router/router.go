package router

import (
	"elastic-search-config-service/handlers"
	"elastic-search-config-service/services"
	"net/http"

	"github.com/gorilla/mux"
)

func NewRouter(esClient *services.ElasticsearchClient) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/index", handlers.PostIndex(esClient)).Methods("POST")
	r.HandleFunc("/index/settings", handlers.PostIndexSettings(esClient)).Methods("POST")
	r.HandleFunc("/{index_name}/documents", handlers.PostDocuments(esClient)).Methods("POST")
	r.HandleFunc("/{index_name}/attributes", handlers.GetIndexAttributesHandler(esClient)).Methods("GET")
	r.HandleFunc("/{index_name}/change_mappings", handlers.ChangeMappings(esClient)).Methods("POST")
	r.HandleFunc("/{index_name}/search", handlers.Search(esClient)).Methods(http.MethodPost)
	// get filter attributes (facets for filtering)
	// search with optional fields
	// TODO: add synonym support

	return r
}
