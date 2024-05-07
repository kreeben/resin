# &#9084; Resin Information Retreival

[Overview](https://github.com/kreeben/resin/blob/master/README.md) | [How to install](https://github.com/kreeben/resin/blob/master/INSTALL.md) | [User guide](https://github.com/kreeben/resin/blob/master/USER-GUIDE.md) 

## Sir.Cmd

Sir.Cmd is a console application for interacting with Resin databases and indices. It runs built-in commands as well as `ICommand` commands 
that you may write yourself.

### Usage

#### `sir truncate`

Removes a collection.

##### Syntax

sir truncate --dataDirectory [dataDirectory] --collection [collectionName]

##### Example

`sir truncate --dataDirectory c:\temp\data --collection wikipedia`

#### `sir truncate-index`

Removes the index of a collection.

##### Syntax

sir truncate-index --dataDirectory [dataDirectory] --collection [collectionName]

##### Example

`sir truncate-index --dataDirectory c:\temp\data --collection wikipedia`

#### `sir optimize`

Creates an optimized index out of all existing segments to make querying faster.

##### Syntax

sir optimize --dataDirectory [dataDirectory] --collection [collectionName] --skip [skip] --take [take] --pageSize [pageSize] --reportFrequency [reportFrequency] --fields [comma-separated_list_of_field_names_to_index]

##### Example

`sir optimize --dataDirectory c:\temp\data --collection wikipedia --skip 0 --take 10000000 --pageSize 100000 --reportFrequency 1000 --fields title,text`

#### `sir slice`

Help function for investigating big files of any type.

##### Syntax

sir slice --sourceFileName [sourceFileName] --resultFileName [resultFileName] --length [bytes_to_read]

#### `sir downloadandindexcommoncrawl`

Downloads and indexes common crawl WAT files.  

##### Syntax

sir downloadandindexcommoncrawl --dataDirectory [path_to_data_directory] --commonCrawlId [commonCrawlId] --workingDirectory [workingDirectory] --collection [collection_name] --skip [skip] --take [take]

##### Example  

`sir downloadandindexcommoncrawl --dataDirectory c:\temp\data --commonCrawlId CC-MAIN-2019-51 workingDirectory d:\ --collection cc_wat --skip 0 --take 1`

#### `sir writewikipedia`

Writes Wikipedia data to a collection but does not create any indices. Indices may be created by issuing a `sir.bat optimize` command. 

##### Syntax

sir writewikipedia --dataDirectory [dataDirectory] --fileName [name_of_gziped_wikipedia_cirrus_search_dump_file] --collection [collection_name]

##### Example  

`sir writewikipedia --dataDirectory c:\temp\data --fileName d:\enwiki-20201026-cirrussearch-content.json.gz --collection wikipedia`

#### `sir indexwikipedia`

Writes Wikipedia data to a collection and creates indices for each field. 

##### Syntax

sir indexwikipedia --dataDirectory [dataDirectory] --fileName [name_of_gziped_wikipedia_cirrus_search_dump_file] --collection wikipedia

##### Example  

`sir indexwikipedia --dataDirectory c:\temp\data --fileName d:\enwiki-20201026-cirrussearch-content.json.gz --collection [collection_name]`

#### `sir indexmnist`

Create an MNSIT image index.  

##### Syntax

sir indexmnist --dataDirectory [dataDirectory] --imageFileName [image_file_name] --labelFileName [label_file_name] --collection [collection_name]

##### Example  

`sir indexmnist --dataDirectory c:\temp\data --imageFileName C:\temp\mnist\train-images.idx3-ubyte --labelFileName C:\temp\mnist\train-labels.idx1-ubyte --collection mnist`

#### `sir validatemnist`

Validate an MNIST image index, i.e. determine its error rate.  

##### Syntax

sir validatemnist --dataDirectory [dataDirectory] --imageFileName [image_file_name] --labelFileName [label_file_name] --collection [collection_name]

##### Example  

`sir validatemnist --dataDirectory c:\temp\data --imageFileName C:\temp\mnist\t10k-images.idx3-ubyte --labelFileName C:\temp\mnist\t10k-labels.idx1-ubyte --collection mnist`

#### `sir your_custom_task`

Implement `Sir.ICommand` from the Sir.Core.dll. The name of your command is also how you reference it through this command-line tool. 
The command name is the class name minus the word "Command", lowercased. 
Drop your DLL file into Sir's bin folder and run it with `sir`

##### Syntax 

sir your_custom_task --your_args [your_args]