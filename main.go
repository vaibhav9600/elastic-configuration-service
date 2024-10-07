package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"elastic-search-config-service/globals"
	"elastic-search-config-service/models"
	"elastic-search-config-service/router"
	"elastic-search-config-service/services"
)

// LoadMappingsFromFile loads the mappings from a JSON file
func LoadMappingsFromFile(filename string) (map[string]models.MappingInfo, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading mappings file: %w", err)
	}

	var mappings map[string]models.MappingInfo
	if err := json.Unmarshal(data, &mappings); err != nil {
		return nil, fmt.Errorf("error unmarshaling mappings: %w", err)
	}

	return mappings, nil
}

func main() {
	//TODO: Handle empty or no file present case
	loadedMappings, err := LoadMappingsFromFile("es_mappings.json")
	if err != nil {
		fmt.Println("Failed to load mappings:", err)
		return
	}

	globals.ESIndexMappings = loadedMappings
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
