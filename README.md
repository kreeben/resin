# &#9084; Resin Search Engine

Overview | [How to install](https://github.com/kreeben/resin/blob/master/INSTALL.md) | [User guide](https://github.com/kreeben/resin/blob/master/USER-GUIDE.md) 

## Resin is a remote HTTP search engine and an embedded library

Resin is a vector space index based search engine that's available as a HTTP service or as an embedded library. 

### How to use

#### Write a document remotely

HTTP POST `[host]/write?collection=[collection]`  
(e.g. http://localhost/write?collection=mycollection)  
Content-Type: application/json  
```
[
	{
		"field1": "value1",
		"field2": "value2"
	}
]
```

#### Write a document locally

```
using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy))
{
    foreach (var document in documents)
    {
        database.Write(document);
    }

    database.Commit();
}
```

#### Query

##### GET query
HTTP GET `[host]/query/?collection=mycollection&q=[my_query]&field=field1&field=field2&select=field1&skip=0&take=10`  
(e.g. http://localhost/write?collection=mycollection&q=value1&field=field1&field=field2&select=field1&skip=0&take=10)  
Accept: application/json  

##### POST query
HTTP POST `[host]/query/?select=field1&skip=0&take=10`  
Content-Type: application/json  
Accept: application/json  

```
{
	"and":
	{
		"collection": "film,music",
		"title": "rocky eye of the tiger",
		"or":
		{
			"title": "rambo",
			"or": 
			{
				"title": "cobra"
				"or":
				{
					"cast": "antonio banderas"
				}			
			}	
		},
		"and":
		{
			"year": 1980,
			"operator": "gt"
		},
		"not":
		{
			"title": "first blood"
		}
	}
}
```

##### Local query

```
using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy))
{
    var queryParser = database.CreateQueryParser();
    var query = queryParser.Parse(collectionId, word, "title", "title", and:true, or:false, label:true);
    var result = database.Read(query, skip: 0, take: 1);
}
```

## Document database
Resin stores data as document collections. It applies your prefered IModel<T>and indexing strategy onto your data while you write and query it. 
The write pipeline produces a set of indices (graphs), one for each document field, that you may interact with by using the Resin read/write JSON HTTP API or programmatically.

## Vector-based indices
Resin indices are binary search trees that create clusters of vectors that are similar to each other, as you populate them with your data. 
When a node is added to the graph its cosine angle, i.e. its similarity to other nodes, determine its position (path) within the graph.

## Performance
Currently, Wikipedia size data sets produce indices capable of sub-second phrase searching. 

## You may also  
- build, validate and optimize indices using the command-line tool [Sir.Cmd](https://github.com/kreeben/resin/blob/master/src/Sir.Cmd/README.md)
- read efficiently by specifying which fields to return in the JSON result
- implement messaging formats such as XML (or any other, really) if JSON is not suitable for your use case
- construct queries that join between fields and even between collections, that you may post as JSON to the read endpoint or create programatically.
- construct any type of indexing scheme that produces any type of embeddings with virtually any dimensionality using either sparse or dense vectors.