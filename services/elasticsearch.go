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

func (es *ElasticsearchClient) GetIndexAttributes(indexName string) ([]string, error) {
	req := esapi.IndicesGetMappingRequest{
		Index: []string{indexName},
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

func (es *ElasticsearchClient) ChangeMappings(indexInfo models.IndexInfo, settings models.SetIndexSettings) error {
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

	properties := make(map[string]interface{})

	// Step 3: Update the mappings
	for _, field := range settings.SearchableAttributes {
		properties[field] = map[string]interface{}{
			"type":  "text",
			"index": true,
		}
	}

	for _, field := range settings.FacetsAttributes {
		dataType := determineDataType(field)
		properties[field] = map[string]interface{}{
			"type":  dataType,
			"index": true,
		}
	}

	for field := range mappingResponse[currentIndex].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{}) {
		if _, ok := properties[field]; !ok {
			properties[field] = map[string]interface{}{
				"type":  "object",
				"index": false,
			}
		}
	}

	// Step 4: Create a new index with the updated mappings
	newIndexName := indexInfo.IndexName + "_new"
	createIndexReq := esapi.IndicesCreateRequest{
		Index: newIndexName,
		Body: strings.NewReader(fmt.Sprintf(`{
			"mappings": {
				"properties": %s
			}
		}`, marshalToJSONString(properties))),
	}
	createIndexRes, err := createIndexReq.Do(context.Background(), es.client)
	if err != nil {
		return err
	}
	defer createIndexRes.Body.Close()
	if createIndexRes.IsError() {
		return fmt.Errorf("error creating new index: %s", createIndexRes.String())
	}

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

	return nil
}

// Helper function to determine the data type of the field
func determineDataType(field string) string {
	// For simplicity, returning types based on the field name
	if strings.Contains(field, "int") || strings.Contains(field, "number") {
		return "integer"
	}
	if strings.Contains(field, "date") {
		return "date"
	}
	return "keyword"
}

// Helper function to marshal properties to JSON string
func marshalToJSONString(data interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}
