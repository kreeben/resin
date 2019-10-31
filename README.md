# &#9084; Resin Extensible Search Engine

Resin is a document database that's been coupled with a search index, but not just any index, this index, can represent any vector space no matter how thick or wide as long as it's a `Sir.IModel`, meaning, you can use it to analyze term based spaces as well as semantic ones, so, almost any index.

## Pluggable, searchable vector spaces

From embeddings extracted from document fields during the tokenization phase of the write session, spaces are
constructed and persisted on disk as bitmaps, made scannable in a streaming fashion so that only a small amount of pressure is put on memory while querying, only what amounts to the size of a single graph node (per thread), which is usually very small, enabling the possibility to scan indices that are larger than memory. 

Spaces are configured by implementing `IModel` or `IStringModel`.

## Write, map, materialize

Main processes of the Resin back-end:

__Write__: documents that consist of keys and values, that are mappable to `IDictionary<string, object>` without corruption, where object is of type int, long, float, datetime, string *, basically a non-nested JSON document (or XML, or something else, it's your choice. There's no recipe, only a method), are persisted to disk, fields turned into vectors through tokenization, each vector added to a graph (see "Balancing") of nodes that each reference one or more documents, each such node appended to a file on disk as part of a segment in a column index that will, by the powers of your platform's parallellism, be scanned during mapping of those queries that target this column.

*) or bit array

Tokenization is configured by implementing `IModel.Tokenize`.

__Map__: a query that represents one or more terms, each term identifying both a column (i.e. key) and a value, is converted into a tree of vectors (through tokenization) where nodes represent a boolean set operations over your space, each query vector compared to the vectors of your space by performing binary search over the nodes of your paged column bitmap files, so, luckily, not to all vectors, only, but this is not guaranteed to always be the case, log(N) x NumOfPages. 

How often more and how many more depends to some degree on how you balanced your tree and to another, hopefully much smaller degree, and this goes for all probabilistic models, and we're probabilistic because two vectors that are not absolutely identical to each other, can be merged (see "Balancing"), on pure chance, but also because other reasons that I'm not going to go into now, but that you're of course free to ask me about at any time and I will answer sincerely, but not necessarily accurately.

__Materialize__: each node in the query tree that recieved a mapping to one or more postings lists ("lists of document references") during the mapping step now materialize their postings, so we can join them with those of their parent, through intersection, union or deletion, while also scoring them and, once the tree's been materialized all the way down to the root and we have reduced the tree to a single list of references, we can __sort__ them by relevance and, finally, materialize a __page__ of documents.

## Sorting

Surprisingly to some, but not all, but certainly to me, once your space approaches big data you find out what is the real problem in search, it's sorting. MF sorting. Who'd have though?

## Balancing

Balancing the binary tree that represents your space is done by adjusting the merge factor ("IdenticalAngle") and the fold factor ("FoldAngle"). 

A node's placement in the index is determined by calculating its angle to the node it most resembles. If the angle is greater than or equal to IdenticalAngle the two nodes merge. If it is not then a new node will be added to the binary tree. In that case, if the angle is greater than FoldAngle, it is added as a left child to the node or, if that slot is taken, to the next left node that has a empty left slot, otherwise as a right child.

IdenticalAngle and FoldAngle are properties of `IModel`.

## Closest matching vector

A query can consist of many sub queries, each can carry a list of query terms. 

Finding a query term's closest matching vector inside a space entails finding the correct column index file, locating the boundaries of each segment, querying those segments by finding the root node, represented on disk as the first block in the segment, deserializing it, calculating the cos angle between the query vector and the index node's vector, determining whether to go left or right based on if the angle is over IModel.FoldAngle or below/equal or calling it because the angle is greater than or equal to IndenticalAngle, which means, nowhere in the segment can there exist a better match than the one we already found.

That's the good news. The bad news is that there are lots of skips. The good news is we can have SSD's. Or if we can't then we can memory map the indices. Like I said, there's no real recipe.

## Read/write performance

The bigger the graph the longer it takes to build, 
because more nodes will need to be traversed before we can find empty slots for new nodes. 
We can improve writing speed by creating many index file segments.

When it comes to querying speed, however, one large graph is better than many segments.

Because the shape of my data might not be the shape of yours, 
you have been given a choice between optimizing for writing or querying. 
There's hope you'll find a good balance between both.

### Sparse/dense

In high dimensions, sparse vectors will enable fast scanning.

In low dimensions, dense vectors will might not impact querying speed negatively.

In a dense space, especially a high dimensional one, 
a high CPU clock frequency is required for decent querying performance, 
as well as lots, and lots of cores.

## APIs

Resin offers both an in-proc, NHibernate-like API, in that there are sessions, a factory, and the notion of a unit of work, as well as fully fledged JSON-friendly HTTP API, in an attempt to follow the principle of what you can do locally your should also be able to do remotely. 

## Apps

- __Sir.HttpServer__: HTTP search service (read, write, query naturally or w/QL)
- __Sir.DbUtil__: write, validate and query via command-line

## Libs (.Net Core 3 apps can embedd and extend these)

- __Sir.KeyValue__: key/value/document System.IO.Stream-based database
- __Sir.VectorSpace__: hardware accellerated computations over and stream based storage of vectors and spaces
- __Sir.Search__: in-proc search engine (SessionFactory, WriteSession, ReadSession)

## Contribute

Code contributions, error reports and suggestions of any kind are most welcome.

## Roadmap

- [x] v0.1a - bag-of-characters vector space language model
- [x] v0.2a - HTTP API
- [x] v0.3a - query language
- [ ] v0.4 - semantic language model
- [ ] v0.5 - image model
- [ ] v1.0 - voice model
- [ ] v2.0 - image-to-voice
- [ ] v2.1 - voice-to-text
- [ ] v2.2 - text-to-image
- [ ] v3.0 - AGI
