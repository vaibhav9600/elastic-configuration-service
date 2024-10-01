package models

type IndexName struct {
	Index string `json:"index_name"`
}

type IndexInfo struct {
	ReadAlias  string
	WriteAlias string
	IndexName  string
}

func GetIndexInfo(index IndexName) IndexInfo {
	return IndexInfo{
		IndexName:  index.Index,
		ReadAlias:  index.Index + "_ReadAlias",
		WriteAlias: index.Index + "_WriteAlias",
	}
}
