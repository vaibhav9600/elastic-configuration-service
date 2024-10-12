# Elasticsearch Configuration Service

This repository provides a service that abstracts the complexities of working with **Elasticsearch**, offering an **Algolia-like** interface for users. The service is designed to make search configuration self-serve, allowing consumers to create and manage Elasticsearch indexes without needing to understand the underlying mechanics of Elasticsearch. The service also adds an extra layer of security, validation, and ease of use, with functionalities for creating, modifying, and managing indexes, mappings, and documents.

## Key Features

- **Index Creation and Management**: Users can create indexes in Elasticsearch through the service, with read and write aliases automatically configured for each index. This allows seamless access and data flow during index migrations.
  
- **Settings Management**: Index settings can be modified with minimal effort, and updates to the index configurations can be applied dynamically.

- **Document Indexing**: Documents can be indexed through the service, which provides an additional validation layer before sending data to Elasticsearch. If no mapping is set for a document, the service automatically configures it.

- **Dynamic Mappings and Attribute Listings**: Users can view the attributes (flattened JSON fields) indexed in Elasticsearch, which are used for specifying searchable and facetable fields.

- **Seamless Mapping Changes**: Consumers can update mappings with near-zero downtime, as the service handles index migration, alias management, and reindexing.

## API Endpoints

Here are the key API endpoints provided by the service:

### 1. **Create Index**
   - **Endpoint**: `/index`
   - **Method**: `POST`
   - **Description**: Create a new index in Elasticsearch with read and write aliases.
   
### 2. **Modify Index Settings**
   - **Endpoint**: `/index/settings`
   - **Method**: `POST`
   - **Description**: Modify settings for an existing Elasticsearch index.

### 3. **Index Documents**
   - **Endpoint**: `/{index_name}/documents`
   - **Method**: `POST`
   - **Description**: Index documents into a specified index. The service automatically validates and configures the mappings if they are not set.

### 4. **Get Index Attributes**
   - **Endpoint**: `/{index_name}/attributes`
   - **Method**: `GET`
   - **Description**: Retrieve a list of attributes (fields) from the index, representing flattened JSON fields. This is useful for identifying searchable and facetable fields.

### 5. **Change Index Mappings**
   - **Endpoint**: `/{index_name}/change_mappings`
   - **Method**: `POST`
   - **Description**: Update the mappings for a specified index. The service automatically handles alias switching and reindexing with minimal downtime.

### 6. **Search Documents**
   - **Endpoint**: `/{index_name}/search`
   - **Method**: `POST`
   - **Description**: Perform a search on a specific index. The search will be executed with an OR condition on all searchable fields, or a subset if specified.

### 7. **Get Facets**
   - **Endpoint**: `/{index_name}/facets`
   - **Method**: `POST`
   - **Description**: Retrieve faceted search data from the specified index.

## How the Service Works

### Index Creation and Aliases
When an index is created through the service:
- **Read and Write Aliases** are automatically configured to allow continuous read and write operations during index migrations.
- Indexes are created with an additional layer of security, ensuring only authorized requests can modify or access the data.

### Index Settings Modification
The service allows users to modify index settings such as refresh intervals, replica counts, and shard configurations with ease. This is useful for optimizing performance based on your use case.

### Document Indexing and Mappings
Documents can be indexed into Elasticsearch via the `/documents` endpoint. Before documents are sent to Elasticsearch, the service performs validation to ensure proper formatting. If a mapping for a document field is not defined, the service automatically configures it based on the following dynamic rules:

- **Searchable Fields**: If a field is marked as searchable, it is indexed as `text`.
- **Searchable and Filterable Fields**: For fields that need both search and filter functionality, they are indexed as both `text` and `keyword`.
- **Filterable-Only Fields**: If a field is meant for filtering only, it is indexed as `keyword`. Even if the original field type is numeric (e.g., `integer`, `float`), it will be temporarily treated as a `keyword`. Support for range-based filtering will be added later.
- **Non-Searchable and Non-Filterable Fields**: If a field is neither searchable nor filterable, its original type is preserved, but both `index` and `doc_values` are disabled to save space.
- **Nested Fields**: Nested fields are not allowed as top-level fields.

### Listing Attributes
Once documents are indexed, the service provides an endpoint to list all the attributes (fields) from the index. These attributes are flattened representations of the JSON fields, using dot notation to denote parent-child relationships. This allows users to easily choose fields for search and faceting purposes.

### Dynamic Mapping Changes
When users want to change the mappings of an existing index, the service manages the following steps automatically:
- A **new index** with updated mappings is created.
- **Read alias** continues to point to the old index, ensuring no downtime.
- **Write alias** is switched to the new index after the mapping is updated.
- The service performs **reindexing** from the old index to the new index.
- Once reindexing is complete, the **read alias** is switched to the new index.
- The old index is submitted for cleanup, which runs a job every 7 days to remove unused indexes.

This approach ensures minimal downtime and avoids manual handling of index migration tasks.

## Rules for Creating Dynamic Mappings

The service follows these rules for creating dynamic mappings:

1. **Searchable Fields**:
   - If a field is searchable, it is indexed as `text`.
  
2. **Searchable and Filterable Fields**:
   - Fields that need to be both searchable and filterable are indexed as both `text` and `keyword`.

3. **Filterable-Only Fields**:
   - Fields that are only used for filtering are indexed as `keyword`. Even numeric fields (e.g., `integer`, `float`, `long`, `date`) are indexed as `keyword`. Future updates will support range-based operations.

4. **Non-Searchable and Non-Filterable Fields**:
   - Fields that are neither searchable nor filterable are indexed as their original type, but both `index` and `doc_values` are disabled.

5. **Nested Fields**:
   - Top-level fields are not allowed to be of type `nested`.

## Assumptions and Considerations

- **Searches**: All searches will use an OR condition across all searchable fields or a subset of fields if specified.
- **Filters**: Filters will be flexible, supporting dynamic operations like `AND`, `OR`, and more advanced filtering criteria.
  
## Conclusion

This Elasticsearch Configuration Service simplifies the management of indexes, documents, and mappings in Elasticsearch. By abstracting the complexities of Elasticsearch, it provides an easy-to-use interface similar to Algolia, ensuring users can configure and manage search functionality without worrying about the underlying mechanics of Elasticsearch.
