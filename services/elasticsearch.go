package services

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"elastic-search-config-service/models"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ElasticsearchClient struct {
	client *elasticsearch.Client
}

// TODO: have proper versioning name support instead of just _new suffix
func NewElasticsearchClient(url string) (*ElasticsearchClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{url},
		Password:  "2Il=zpAuWSseDPLb+d7+",
		Username:  "elastic",
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	res, err := client.Ping()
	fmt.Println("ping res", res)
	fmt.Println("ping err", err)
	return &ElasticsearchClient{client: client}, nil
}

func (es *ElasticsearchClient) CreateIndexAndAliases(index models.IndexInfo) error {
	req := esapi.IndicesCreateRequest{
		Index: index.IndexName,
	}
	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("error creating index: %s", res.String())
	}
	fmt.Println(res)

	// TODO: make this api idempotent see case when index created but alias not created
	aliasBody := fmt.Sprintf(`{
		"actions": [
			{"add": {"index": "%s", "alias": "%s"}},
			{"add": {"index": "%s", "alias": "%s"}}
		]
	}`, index.IndexName, index.ReadAlias, index.IndexName, index.WriteAlias)

	aliasReq := esapi.IndicesUpdateAliasesRequest{
		Body: strings.NewReader(aliasBody),
	}
	aliasRes, err := aliasReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer aliasRes.Body.Close()
	if aliasRes.IsError() {
		return fmt.Errorf("error creating aliases: %s", aliasRes.String())
	}
	fmt.Println("Aliases created:", aliasRes)
	return nil
}

func (es *ElasticsearchClient) UpdateIndexSettings(settings map[string]interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(settings); err != nil {
		return err
	}
	req := esapi.IndicesPutSettingsRequest{
		Body: &buf,
	}
	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("error creating index: %s", res.String())
	}
	fmt.Println(res)
	return err
}

// TODO: retry on writes
func (es *ElasticsearchClient) IndexDocuments(indexName string, documents []models.Document) error {
	//TODO: remarks use bulk api here
	for _, doc := range documents {
		body, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		fmt.Println(body)
		req := esapi.IndexRequest{
			Index:      indexName,
			DocumentID: doc.ID,
			Body:       bytes.NewReader(body),
			Refresh:    "true",
		}
		res, err := req.Do(context.Background(), es.client)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			return fmt.Errorf("error creating index: %s", res.String())
		}
		fmt.Println(res)
	}
	return nil
}

//tag:info https://stackoverflow.com/questions/41382627/do-you-need-to-delete-elasticsearch-aliases
func (es *ElasticsearchClient) GetIndexAttributes(ind models.IndexInfo) ([]string, error) {
	req := esapi.IndicesGetMappingRequest{
		Index: []string{ind.ReadAlias},
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error getting index mapping: %s", res.String())
	}

	var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}

	// Extract and flatten the mapping fields
	attributes := flattenMappings(response)
	return attributes, nil
}

// Helper function to flatten the mappings into a list of attribute strings
func flattenMappings(mapping map[string]interface{}) []string {
	var attributes []string

	for _, indexData := range mapping {
		properties, found := indexData.(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
		if !found {
			continue
		}
		attributes = append(attributes, extractProperties(properties, "")...)
	}

	return attributes
}

// Recursively extract property paths
func extractProperties(properties map[string]interface{}, prefix string) []string {
	var result []string

	for key, value := range properties {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}

		valueMap, ok := value.(map[string]interface{})
		if ok {
			if subProps, exists := valueMap["properties"].(map[string]interface{}); exists {
				result = append(result, extractProperties(subProps, fullKey)...)
			} else {
				result = append(result, fullKey)
			}
		} else {
			result = append(result, fullKey)
		}
	}

	return result
}

// TODO: add support for custom analyzers
func (es *ElasticsearchClient) ChangeMappings(indexInfo models.IndexInfo, settings models.IndexSettings) error {
	// Step 1: Get the current index from the read alias
	getAliasReq := esapi.IndicesGetAliasRequest{
		Name: []string{indexInfo.ReadAlias},
	}
	getAliasRes, err := getAliasReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer getAliasRes.Body.Close()
	if getAliasRes.IsError() {
		return fmt.Errorf("error getting alias: %s", getAliasRes.String())
	}

	var aliasResponse map[string]interface{}
	if err := json.NewDecoder(getAliasRes.Body).Decode(&aliasResponse); err != nil {
		return err
	}

	var currentIndex string
	for index := range aliasResponse {
		currentIndex = index
		break
	}

	// Step 2: Get the current mappings for the index
	getMappingReq := esapi.IndicesGetMappingRequest{
		Index: []string{currentIndex},
	}
	getMappingRes, err := getMappingReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer getMappingRes.Body.Close()
	if getMappingRes.IsError() {
		return fmt.Errorf("error getting index mappings: %s", getMappingRes.String())
	}

	var mappingResponse map[string]interface{}
	if err := json.NewDecoder(getMappingRes.Body).Decode(&mappingResponse); err != nil {
		return err
	}

	fmt.Println(marshalToJSONString(mappingResponse))

	properties, _ := createDynamicMapping(currentIndex, mappingResponse, settings)

	newMappings := map[string]interface{}{
		"properties": properties["properties"].(map[string]interface{}),
	}

	// Step 4: Create a new index with the updated mappings
	fmt.Println(marshalToJSONString(newMappings))
	newIndexName := indexInfo.IndexName + "_new"
	var buf bytes.Buffer
	query := map[string]interface{}{
		"mappings": newMappings,
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return err
	}
	createIndexReq := esapi.IndicesCreateRequest{
		Index: newIndexName,
		Body:  &buf,
	}
	createIndexRes, err := createIndexReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer createIndexRes.Body.Close()
	if createIndexRes.IsError() {
		return fmt.Errorf("error creating new index: %s", createIndexRes.String())
	}

	// TODO: explore if what would happen if we point read alias to two indices
	// Step 5: Point write alias to new index
	updateAliasReq := esapi.IndicesUpdateAliasesRequest{
		Body: strings.NewReader(fmt.Sprintf(`{
			"actions": [
				{"remove": {"index": "%s", "alias": "%s"}},
				{"add": {"index": "%s", "alias": "%s"}}
			]
		}`, currentIndex, indexInfo.WriteAlias, newIndexName, indexInfo.WriteAlias)),
	}
	updateAliasRes, err := updateAliasReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer updateAliasRes.Body.Close()
	if updateAliasRes.IsError() {
		return fmt.Errorf("error updating aliases: %s", updateAliasRes.String())
	}

	// Step 6: Reindex documents to the new index
	waitForCompletion := true
	reindexReq := esapi.ReindexRequest{
		Body: strings.NewReader(fmt.Sprintf(`{
			"source": {"index": "%s"},
			"dest": {"index": "%s"}
		}`, currentIndex, newIndexName)),
		WaitForCompletion: &waitForCompletion,
	}
	reindexRes, err := reindexReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer reindexRes.Body.Close()
	if reindexRes.IsError() {
		return fmt.Errorf("error during reindexing: %s", reindexRes.String())
	}

	// Step 7: Update read alias to point to the new index
	finalUpdateAliasReq := esapi.IndicesUpdateAliasesRequest{
		Body: strings.NewReader(fmt.Sprintf(`{
			"actions": [
				{"remove": {"index": "%s", "alias": "%s"}},
				{"add": {"index": "%s", "alias": "%s"}}
			]
		}`, currentIndex, indexInfo.ReadAlias, newIndexName, indexInfo.ReadAlias)),
	}
	finalUpdateAliasRes, err := finalUpdateAliasReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer finalUpdateAliasRes.Body.Close()
	if finalUpdateAliasRes.IsError() {
		return fmt.Errorf("error updating read alias: %s", finalUpdateAliasRes.String())
	}

	// Step 8: Log the new mappings
	fmt.Printf("New mappings for index %s: %v\n", newIndexName, properties)

	// TODO: setup pipelines for cleaning up old indexes
	return nil
}

func createDynamicMapping(index string, existingMapping map[string]interface{}, settings models.IndexSettings) (map[string]interface{}, error) {
	newMapping := make(map[string]interface{})

	// Helper function to check if a field is in a list
	contains := func(list []string, item string) bool {
		for _, v := range list {
			if v == item {
				return true
			}
		}
		return false
	}

	// Helper function to check if any child field is searchable or filterable
	hasSearchableOrFilterableChild := func(prefix string, fields map[string]interface{}) bool {
		for key := range fields {
			fullPath := prefix + key
			if contains(settings.SearchableAttributes, fullPath) || contains(settings.FacetAttributes, fullPath) {
				return true
			}
		}
		return false
	}

	// Helper function to create field mapping
	createFieldMapping := func(fieldName string, fieldType string) map[string]interface{} {
		mapping := map[string]interface{}{}

		isSearchable := contains(settings.SearchableAttributes, fieldName)
		isFilterable := contains(settings.FacetAttributes, fieldName)

		if isSearchable && isFilterable {
			mapping["type"] = "text"
			mapping["fields"] = map[string]interface{}{
				"keyword": map[string]interface{}{
					"type":         "keyword",
					"ignore_above": 256,
				},
			}
		} else if isSearchable {
			mapping["type"] = "text"
		} else if isFilterable { //TODO: see if we can use inherent data types, will be more suitable with range
			mapping["type"] = "keyword"
		} else {
			//TODO: add support for range based queries ref: https://stackoverflow.com/questions/47542363/should-i-choose-datatype-of-keyword-or-long-integer-for-document-personid-in-e
			// https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#map-ids-as-keyword
			if fieldType == "text" {
				mapping["type"] = fieldType
				mapping["index"] = false
			} else {
				mapping["type"] = fieldType
				mapping["index"] = false
				mapping["doc_values"] = false
			}
		}

		return mapping
	}

	// Recursive function to process nested fields
	var processFields func(string, map[string]interface{}) map[string]interface{}
	processFields = func(prefix string, fields map[string]interface{}) map[string]interface{} {
		result := make(map[string]interface{})

		for key, value := range fields {
			fullPath := prefix + key
			if subProperties, ok := value.(map[string]interface{})["properties"]; ok {
				if hasSearchableOrFilterableChild(fullPath+".", subProperties.(map[string]interface{})) {
					result[key] = map[string]interface{}{
						"type":       "nested",
						"properties": processFields(fullPath+".", subProperties.(map[string]interface{})),
					}
				} else {
					result[key] = map[string]interface{}{
						"properties": processFields(fullPath+".", subProperties.(map[string]interface{})),
					}
				}
			} else {
				fieldType := value.(map[string]interface{})["type"].(string)
				result[key] = createFieldMapping(fullPath, fieldType)
			}
		}

		return result
	}

	// Process the root level fields
	rootFields := existingMapping[index].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
	properties := processFields("", rootFields)

	newMapping["properties"] = properties
	return newMapping, nil
}

// Helper function to marshal properties to JSON string
func marshalToJSONString(data interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}
