package models

// order of searchable attributes will decide boosting
type SearchableAttributes []string

// TODO: to apply filters on a field apply facets
// support for dynamic facets is not present at the moment
type FacetsAttributes []string

type IndexSettings struct {
	SearchableAttributes SearchableAttributes `json:"searchable_attributes"`
	FacetAttributes      FacetsAttributes     `json:"facet_attributes"`
}
