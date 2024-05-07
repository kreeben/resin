# &#9084; Resin.Search

[Overview](https://github.com/kreeben/resin/blob/master/README.md) | [How to install](https://github.com/kreeben/resin/blob/master/INSTALL.md) | [User guide](https://github.com/kreeben/resin/blob/master/USER-GUIDE.md) 

## Sir.HttpServer

Sir.HttpServer is a Kestrel application that serves both a HTML search (and result) page and a HTTP JSON read/write API.

### HTTP API

#### Write a document

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

