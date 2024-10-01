package models

type Document struct {
	ID      string      `json:"id"`
	Content interface{} `json:"content"`
}
