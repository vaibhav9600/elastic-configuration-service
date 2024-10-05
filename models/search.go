package models

type SearchConfig struct {
	Attribute []string `json:"names"`
	Boost     int      `json:"boost"`
}

type filterUnit struct {
	Field  string   `json:"field"`
	Values []string `json:"values"`
}

// filter is a collection of filterUnits
type Filter []filterUnit

// TODO: store configuration in db
var ConstMap = map[string][]string{
	"content.deeply_nested.first_name":   {"text", "keyword"},
	"content.deeply_nested.last_name":    {"text", "keyword"},
	"content.deeply_nested.sample_float": {"float"},
	"content.deeply_nested.structure":    {"long"},
	"content.deeply_nested.time_Stamp":   {"date"},
	"id":                                 {"text", "keyword"},
}

// TODO: create advance filters by parsing string
type SearchReq struct {
	IndexName    string         `json:"-"`
	SearchConfig []SearchConfig `json:"search_attribute"`
	SearchString string         `json:"search_string"`
	PageSize     uint32         `json:"page_size"`
	Cursor       uint32         `json:"cursor"`
	Filter       Filter         `json:"filter"`
}

// FieldMapping defines the mapping for a field in the query
type FieldMapping struct {
	Path     string   // The path of the field in the Elasticsearch index
	DataType []string // The data type of the field
	IsNested bool     // Indicates if the field is nested
}

// QueryBuilder helps build Elasticsearch queries
type QueryBuilder struct {
	FieldMappings map[string]FieldMapping
}

// NewQueryBuilder creates a new QueryBuilder instance
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		FieldMappings: map[string]FieldMapping{
			"content.deeply_nested.first_name":   {Path: "content.deeply_nested", DataType: []string{"text", "keyword"}, IsNested: true},
			"content.deeply_nested.last_name":    {Path: "content.deeply_nested", DataType: []string{"keyword"}, IsNested: true},
			"content.deeply_nested.sample_float": {Path: "content.deeply_nested", DataType: []string{"float"}, IsNested: true},
			"content.deeply_nested.structure":    {Path: "content.deeply_nested", DataType: []string{"integer"}, IsNested: true},
			"content.deeply_nested.time_stamp":   {Path: "content.deeply_nested", DataType: []string{"date"}, IsNested: true},
			"id":                                 {Path: "", DataType: []string{"text", "keyword"}, IsNested: false},
		},
	}
}
