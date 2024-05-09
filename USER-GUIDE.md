# &#9084; Resin Search Engine

[Overview](https://github.com/kreeben/resin/blob/master/README.md) | [How to install](https://github.com/kreeben/resin/blob/master/INSTALL.md) | User guide 

## User guide

### Use Resin locally and programmatically
[Overview](https://github.com/kreeben/resin/blob/master/README.md).

### Use Resin through the command-line tool
[How to use the command-line tool](https://github.com/kreeben/resin/blob/master/src/Sir.Cmd/README.md).

### Sir.HttpServer
[How to use Resin's HTTP API](https://github.com/kreeben/resin/blob/master/src/Sir.HttpServer/README.md).
[How to install](https://github.com/kreeben/resin/blob/master/INSTALL.md)

### How to index Wikipedia

#### 1. Download Cirrus search engine JSON backup file

[Download](https://dumps.wikimedia.org/other/cirrussearch/current/) any file that with the word "content" in its file name.

Don't extract it. We'll be reading from the compressed file.

#### 2. Create a data directory on your local storage

E.g.  

´mkdir c:\temp\data\´

#### 3. Store Wikipedia documents as Resin documents

Issue the following Sir.Cmd command:

`.\sir.bat storewikipedia --dataDirectory c:\temp\data --fileName d:\enwiki-20201026-cirrussearch-content.json.gz --collection wikipedia`

#### 4. Create indices

To create indices from the "text" and "title" fields of your Resin documents and segmented them into pages of 100K documents, 
issue the following command:  

`.\sir.bat optimize --dataDirectory c:\temp\data --collection wikipedia --skip 0 --take 10000000 --pageSize 100000 --reportFrequency 1000 --fields title,text`

Launch Sir.HttpServer and use a HTTP client like Postman to query your Wikipedia collection, or use the web GUI, as described [here](https://github.com/kreeben/resin/blob/master/src/Sir.HttpServer/README.md).