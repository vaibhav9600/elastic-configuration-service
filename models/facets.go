package models

import "encoding/json"

type FacetInfo struct {
	Key   string `json:"key"`
	Field string `json:"field"`
	Size  uint32 `json:"size"`
}

type FacetListingRequest struct {
	Facets []FacetInfo `json:"facets"`
}

type FacetResponse struct {
	Aggregations map[string]json.RawMessage `json:"aggregations"`
}

type FacetAggregation struct {
	Buckets []FacetBucket `json:"buckets"`
}

type FacetBucket struct {
	Key      interface{} `json:"key"`
	DocCount int         `json:"doc_count"`
}

type DynamicFacetResponse struct {
	FacetData map[string][]FacetValue `json:"facet_data"`
}

type FacetValue struct {
	Value    interface{} `json:"value"`
	DocCount int         `json:"doc_count"`
}

var AggregatableTypes = map[string]struct{}{
	"keyword": {},
	"integer": {},
	"float":   {},
	"double":  {},
	"long":    {},
	"date":    {},
}
