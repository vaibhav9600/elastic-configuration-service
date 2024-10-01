package main

import (
	"log"
	"net/http"

	"elastic-search-config-service/router"
	"elastic-search-config-service/services"
)

func main() {
	// Initialize Elasticsearch client
	esClient, err := services.NewElasticsearchClient("https://localhost:9200")
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	// Initialize router
	r := router.NewRouter(esClient)

	// Start server
	log.Println("Server is running on :1234")
	log.Fatal(http.ListenAndServe(":1234", r))
}
