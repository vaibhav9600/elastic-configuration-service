package services

import (
	"bytes"
	"context"
	"elastic-search-config-service/models"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func generateFacetAggregations(facetReq models.FacetListingRequest, qb *models.QueryBuilder) map[string]interface{} {
	aggregations := make(map[string]interface{})

	for _, facet := range facetReq.Facets {
		fieldMapping, ok := qb.FieldMappings[facet.Field]
		if !ok {
			// Skip fields that do not exist in the field mappings
			continue
		}

		// Check if the data type is aggregatable
		aggregatable := false
		for _, dataType := range fieldMapping.DataType {
			if _, exists := models.AggregatableTypes[dataType]; exists {
				aggregatable = true
				break
			}
		}

		if !aggregatable {
			// Skip fields that are not aggregatable
			continue
		}

		fieldName := facet.Field
		if len(fieldMapping.DataType) > 1 {
			// If there are multiple types, use the first type as the base field
			if len(fieldMapping.DataType) > 1 {
				for _, dataType := range fieldMapping.DataType {
					if dataType == "keyword" {
						fieldName += ".keyword"
						break
					}
				}
			}
		}

		if fieldMapping.IsNested {
			// Handle nested field aggregation
			aggregations[facet.Key] = map[string]interface{}{
				"nested": map[string]interface{}{
					"path": fieldMapping.Path,
				},
				"aggs": map[string]interface{}{
					"facet_values": map[string]interface{}{
						"terms": map[string]interface{}{
							"field": fieldName,
							"size":  facet.Size,
						},
					},
				},
			}
		} else {
			// Handle regular field aggregation
			aggregations[facet.Key] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": fieldName,
					"size":  facet.Size,
				},
			}
		}
	}
	return aggregations
}

// TODO: Test nested within nested search , filtering and faceting
func (es *ElasticsearchClient) FetchFacetData(facetReq models.FacetListingRequest, qb *models.QueryBuilder) (*models.DynamicFacetResponse, error) {
	// Construct Elasticsearch request payload
	aggregations := generateFacetAggregations(facetReq, qb)
	reqBody := map[string]interface{}{
		"size":         0, // Set size to 0 since we only need aggregations
		"aggregations": aggregations,
	}
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(reqBody); err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: []string{"test_data_test"},
		Body:  &buf,
	}
	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("error creating index: %s", res.String())
	}
	fmt.Println(res)

	var esResp models.FacetResponse
	// var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return nil, err
	}

	// Construct dynamic response structure
	dynamicResponse := &models.DynamicFacetResponse{FacetData: make(map[string][]models.FacetValue)}

	keyToFieldMap := make(map[string]string)
	for _, facet := range facetReq.Facets {
		keyToFieldMap[facet.Key] = facet.Field
	}

	for key, rawAgg := range esResp.Aggregations {
		field, fieldExists := keyToFieldMap[key]
		if !fieldExists {
			continue
		}

		fieldMapping, ok := qb.FieldMappings[field]
		if !ok {
			continue
		}

		var values []models.FacetValue
		if fieldMapping.IsNested {
			// Handle parsing for nested aggregation
			var nestedAgg struct {
				FacetValues models.FacetAggregation `json:"facet_values"`
			}
			if err := json.Unmarshal(rawAgg, &nestedAgg); err != nil {
				return nil, err
			}
			for _, bucket := range nestedAgg.FacetValues.Buckets {
				values = append(values, models.FacetValue{
					Value:    bucket.Key,
					DocCount: bucket.DocCount,
				})
			}
		} else {
			// Handle parsing for non-nested aggregation
			var facetAgg models.FacetAggregation
			if err := json.Unmarshal(rawAgg, &facetAgg); err != nil {
				return nil, err
			}
			for _, bucket := range facetAgg.Buckets {
				values = append(values, models.FacetValue{
					Value:    bucket.Key,
					DocCount: bucket.DocCount,
				})
			}
		}

		dynamicResponse.FacetData[key] = values
	}

	return dynamicResponse, nil
}

func (es *ElasticsearchClient) GetFacetListing(index string, reqPayload models.FacetListingRequest) (models.DynamicFacetResponse, error) {
	queryBuilder := models.NewQueryBuilder()
	facetResponse, err := es.FetchFacetData(reqPayload, queryBuilder)
	if err != nil {
		log.Fatalf("Error fetching facet data: %v", err)
	}
	return *facetResponse, err
}
