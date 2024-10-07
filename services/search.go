package services

import (
	"bytes"
	"context"
	"elastic-search-config-service/models"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func (es *ElasticsearchClient) Search(payload models.SearchReq) (map[string]interface{}, error) {
	ind := models.GetIndexInfo(models.IndexName{Index: payload.IndexName})
	// form query here
	query, queryBuf, err := es.BuildSearchQuery(payload)
	if err != nil {
		return nil, fmt.Errorf("some error occurred while building search query: %w", err)
	}
	fmt.Println(marshalToJSONString(query))
	// return nil, nil
	req := esapi.SearchRequest{
		Index: []string{ind.ReadAlias},
		Body:  &queryBuf,
	}
	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("error creating index: %s", res.String())
	}
	var searchResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, err
	}

	fmt.Println(searchResponse)

	return searchResponse["hits"].(map[string]interface{}), nil
}

func (es *ElasticsearchClient) BuildSearchQuery(reqPayload models.SearchReq) (map[string]interface{}, bytes.Buffer, error) {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": es.getSearchQueryHelper(reqPayload),
		"from":  reqPayload.Cursor,
		"size":  reqPayload.PageSize,
		// "sort": getSortingData(reqPayload),
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return query, buf, err
	}

	return query, buf, nil
}

func (es *ElasticsearchClient) getSearchQueryHelper(reqPayload models.SearchReq) map[string]interface{} {
	normalizeBoostValues(&reqPayload.SearchConfig)

	boolQuery := make(map[string]interface{})
	// qb := models.NewQueryBuilder()
	qb, err := es.GetMappingBuilder(models.GetIndexInfo(models.IndexName{Index: reqPayload.IndexName}))
	if err != nil {
		return map[string]interface{}{}
	}
	if should := generateElasticsearchSearch(&qb, reqPayload); should != nil {
		boolQuery["should"] = should
		boolQuery["minimum_should_match"] = 1 // later we will play around with this
	}
	if filter, _ := generateElasticsearchFilter(&qb, reqPayload.Filter); filter != nil {
		boolQuery["filter"] = filter
	}

	return map[string]interface{}{
		"bool": boolQuery,
	}
}

func generateElasticsearchFilter(qb *models.QueryBuilder, f models.Filter) (map[string]interface{}, error) {
	if len(f) == 0 {
		return nil, fmt.Errorf("no filters provided")
	}

	esFilter := make(map[string]interface{})
	shouldClauses := make([]map[string]interface{}, 0)

	for _, filterUnit := range f {
		fieldMapping, exists := qb.FieldMappings[filterUnit.Field]
		if !exists {
			return nil, fmt.Errorf("field %s does not exist in field mappings", filterUnit.Field)
		}

		if len(filterUnit.Values) == 0 {
			return nil, fmt.Errorf("no values for field %s", filterUnit.Field)
		}

		var shouldClause map[string]interface{}

		if fieldMapping.IsNested {
			// Create a nested clause for nested fields
			nestedQuery := make(map[string]interface{})
			nestedQuery["path"] = fieldMapping.Path // Use the nested path from the mapping

			// Handle nested fields with possible `.keyword` suffix
			queryMap := make(map[string]interface{})
			fieldName := filterUnit.Field
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
			// Handle single data type case
			if len(filterUnit.Values) == 1 {
				queryMap[fieldName] = filterUnit.Values[0]
			} else {
				queryMap[fieldName] = filterUnit.Values
			}

			// Add the query map to the nested query
			if len(filterUnit.Values) == 1 {
				nestedQuery["query"] = map[string]interface{}{
					"term": queryMap,
				}
			} else {
				nestedQuery["query"] = map[string]interface{}{
					"terms": queryMap,
				}
			}

			shouldClause = map[string]interface{}{
				"nested": nestedQuery,
			}
		} else {
			// Handle standard fields with `.keyword` for multi-type fields
			fieldName := filterUnit.Field
			if len(fieldMapping.DataType) > 1 {
				for _, dataType := range fieldMapping.DataType {
					if dataType == "keyword" {
						fieldName += ".keyword"
						break
					}
				}
			}

			if len(fieldMapping.DataType) > 1 {
				shouldClause = map[string]interface{}{
					"terms": map[string]interface{}{
						fieldName: filterUnit.Values,
					},
				}
			} else {
				if len(filterUnit.Values) == 1 {
					shouldClause = map[string]interface{}{
						"term": map[string]interface{}{
							fieldName: filterUnit.Values[0],
						},
					}
				} else {
					shouldClause = map[string]interface{}{
						"terms": map[string]interface{}{
							fieldName: filterUnit.Values,
						},
					}
				}
			}
		}

		// Add the should clause to the list
		shouldClauses = append(shouldClauses, shouldClause)
	}

	// Build the final filter structure
	esFilter["bool"] = map[string]interface{}{
		"must": shouldClauses,
	}

	return esFilter, nil
}

func generateElasticsearchSearch(queryBuilder *models.QueryBuilder, reqPayload models.SearchReq) []map[string]interface{} {
	var shouldClauses []map[string]interface{}

	for _, config := range reqPayload.SearchConfig {
		for _, attribute := range config.Attribute {
			fieldMapping, exists := queryBuilder.FieldMappings[attribute]
			if !exists { //TODO: check for if field is searchable
				continue // Skip attributes not present in the FieldMappings
			}

			// Generate query for different field types
			var matchQuery map[string]interface{}

			if fieldMapping.IsNested {
				// Handle nested fields
				matchQuery = map[string]interface{}{
					"nested": map[string]interface{}{
						"path": fieldMapping.Path,
						"query": map[string]interface{}{
							"bool": map[string]interface{}{
								"should": generateMatchQueries(attribute, reqPayload.SearchString, config.Boost),
							},
						},
					},
				}
			} else {
				// Handle non-nested fields
				matchQuery = map[string]interface{}{
					"bool": map[string]interface{}{
						"should": generateMatchQueries(attribute, reqPayload.SearchString, config.Boost),
					},
				}
			}

			// Add the query to the should clauses
			shouldClauses = append(shouldClauses, matchQuery)
		}
	}

	return shouldClauses
}

func generateMatchQueries(attribute, searchString string, boost int) []map[string]interface{} {
	// Convert boost to a float to apply fractional values
	boostValue := float64(boost)

	// Generate multiple match queries for different matching types
	return []map[string]interface{}{
		{
			"match_phrase": map[string]interface{}{
				attribute: map[string]interface{}{
					"query": searchString,
					"boost": boostValue * 1.0, // Full boost for exact phrase matches
				},
			},
		},
		{
			// TODO: analysis if we are using match with fuzziness would it cover token matching?
			"match": map[string]interface{}{
				attribute: map[string]interface{}{
					"query":     searchString,
					"fuzziness": "AUTO",
					"boost":     boostValue * 0.8, // 80% of the original boost for fuzzy matches
				},
			},
		},
		{
			"match_bool_prefix": map[string]interface{}{
				attribute: map[string]interface{}{
					"query": searchString,
					"boost": boostValue * 0.5, // 50% of the original boost for prefix matches
				},
			},
		},
		// TOken matching not required
		// {
		// 	"match": map[string]interface{}{
		// 		attribute: map[string]interface{}{
		// 			"query": searchString,
		// 			"boost": boostValue * 0.3, // 30% of the original boost for simple token matching
		// 		},
		// 	},
		// },
	}
}

func normalizeBoostValues(searchConfigs *[]models.SearchConfig) {
	maxBoost := 0
	for _, config := range *searchConfigs {
		if config.Boost > maxBoost {
			maxBoost = config.Boost
		}
	}

	for i, config := range *searchConfigs {
		if maxBoost > 0 {
			(*searchConfigs)[i].Boost = int(float64(config.Boost) / float64(maxBoost) * 10) // Normalize boost to a scale from 0 to 10
		}
	}
}
