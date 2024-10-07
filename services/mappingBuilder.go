package services

import (
	"context"
	"elastic-search-config-service/globals"
	"elastic-search-config-service/models"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/elastic/go-elasticsearch/v8/esapi"
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

// processMapping recursively processes the Elasticsearch mapping
func processMapping(properties map[string]interface{}, prefix string, fieldMappings map[string]models.FieldMapping) error {
	for field, mapping := range properties {
		mappingMap := mapping.(map[string]interface{})
		currentPath := prefix
		if prefix != "" {
			currentPath += "."
		}
		currentPath += field

		if nestedProps, ok := mappingMap["properties"].(map[string]interface{}); ok {
			// Handle nested fields
			isNested := mappingMap["type"] == "nested"
			if err := processMapping(nestedProps, currentPath, fieldMappings); err != nil {
				return err
			}

			// Update nested status for child fields
			if isNested {
				updateNestedStatus(currentPath, fieldMappings)
			}
		} else {
			// Handle leaf fields
			dataTypes := []string{mappingMap["type"].(string)}
			if fields, ok := mappingMap["fields"].(map[string]interface{}); ok {
				for subField, subMapping := range fields {
					dataTypes = append(dataTypes, subMapping.(map[string]interface{})["type"].(string))
					if subField == "keyword" {
						dataTypes = append(dataTypes, "keyword")
					}
				}
			}

			fieldMappings[currentPath] = models.FieldMapping{
				Path:     strings.Join(strings.Split(currentPath, ".")[0:len(strings.Split(currentPath, "."))-1], "."),
				DataType: dataTypes,
				IsNested: false,
			}
		}
	}
	return nil
}

// updateNestedStatus updates the IsNested status for all fields under a nested path
func updateNestedStatus(nestedPath string, fieldMappings map[string]models.FieldMapping) {
	for field, mapping := range fieldMappings {
		if strings.HasPrefix(field, nestedPath+".") {
			mapping.IsNested = true
			fieldMappings[field] = mapping
		}
	}
}

// InferMappingsFromES creates MappingInfo from Elasticsearch mapping
func (es *ElasticsearchClient) InferMappingsFromES(indexName string) (*models.MappingInfo, error) {
	getMappingReq := esapi.IndicesGetMappingRequest{
		Index: []string{indexName},
	}

	getMappingRes, err := getMappingReq.Do(context.Background(), es.client)
	if err != nil {
		return nil, err
	}
	defer getMappingRes.Body.Close()
	if getMappingRes.IsError() {
		return nil, fmt.Errorf("error creating index: %s", getMappingRes.String())
	}

	var mappingResponse map[string]interface{}
	if err := json.NewDecoder(getMappingRes.Body).Decode(&mappingResponse); err != nil {
		return nil, err
	}
	var actualIndexName string
	for key := range mappingResponse {
		actualIndexName = key // Get the actual index name, assuming there's only one
		break
	}

	fieldMappings := make(map[string]models.FieldMapping)
	if err := processMapping(mappingResponse[actualIndexName].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{}), "", fieldMappings); err != nil {
		return nil, err
	}

	return &models.MappingInfo{
		IndexName:     indexName,
		FieldMappings: fieldMappings,
	}, nil
}

// Thread-safe file operations
var fileMutex sync.Mutex

// createBackup creates a backup of the existing file
func createBackup(filename string) error {
	backupName := filename + ".backup"
	input, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	err = os.WriteFile(backupName, input, 0644)
	if err != nil {
		return err
	}

	return nil
}

// SaveMappingsToFile saves the mappings to a JSON file, appending new entries
// and updating existing ones
func SaveMappingsToFile(newMappings map[string]models.MappingInfo, filename string) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Create file if it doesn't exist
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return saveToFile(newMappings, filename)
	}

	// Load existing mappings
	existingMappings, err := LoadMappingsFromFile(filename)
	if err != nil {
		// If there's an error reading the file (e.g., corrupted JSON),
		// backup the existing file and create a new one
		if err := createBackup(filename); err != nil {
			return fmt.Errorf("error creating backup: %w", err)
		}
		return saveToFile(newMappings, filename)
	}

	// Merge new mappings with existing ones
	for indexName, mapping := range newMappings {
		existingMappings[indexName] = mapping
	}

	// Save merged mappings back to file
	return saveToFile(existingMappings, filename)
}

// saveToFile handles the actual file writing
func saveToFile(mappings map[string]models.MappingInfo, filename string) error {
	data, err := json.MarshalIndent(mappings, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshaling mappings: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("error writing mappings file: %w", err)
	}

	return nil
}

func (es *ElasticsearchClient) GetQueryBuilder(indexName string) (*models.QueryBuilder, error) {
	mappingInfo, exists := globals.ESIndexMappings[indexName]
	if !exists {
		mappingInfo, err := es.InferMappingsFromES(indexName)
		if err != nil {
			return nil, err
		}

		// Store mappings in a map with index name as key
		allMappings := map[string]models.MappingInfo{
			mappingInfo.IndexName: *mappingInfo,
		}

		globals.ESIndexMappings = allMappings

		// Save mappings to file
		if err := SaveMappingsToFile(allMappings, "es_mappings.json"); err != nil {
			return nil, err
		}
	}

	mappingInfo, exists = globals.ESIndexMappings[indexName]
	if !exists {
		return nil, fmt.Errorf("no mapping found for index: %s", indexName)
	}

	return &models.QueryBuilder{
		FieldMappings: mappingInfo.FieldMappings,
	}, nil
}

func (es *ElasticsearchClient) GetMappingBuilder(ind models.IndexInfo) (models.QueryBuilder, error) {
	queryBuilder, err := es.GetQueryBuilder(ind.ReadAlias)
	if err != nil {
		return models.QueryBuilder{}, fmt.Errorf("some error occurred while retrieving mappings info")
	}

	return *queryBuilder, nil
}
