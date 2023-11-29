using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Internal;
using Sir.Documents;
using Sir.IO;
using Sir.Strings;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text.RegularExpressions;

namespace Sir.StringTests
{
    public class BagOfCharsModelTests
    {
        private ILoggerFactory _loggerFactory;
        private SessionFactory _sessionFactory;

        private readonly string[] _data = new string[] { "Ferriman–Gallwey score", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope", "cat" };

        [Test]
        public void Can_traverse_index_in_memory()
        {
            var model = new BagOfCharsModel();

            var index = model.CreateTree(new LogStructuredIndexingStrategy(model), _data);

            Debug.WriteLine(PathFinder.Visualize(index));

            Assert.DoesNotThrow(() =>
            {
                foreach (var word in _data)
                {
                    foreach (var queryVector in model.CreateEmbedding(word, true))
                    {
                        var hit = PathFinder.ClosestMatch(index, queryVector, model);

                        if (hit == null)
                        {
                            throw new Exception($"unable to find {word} in index.");
                        }

                        if (hit.Score < model.IdenticalAngle)
                        {
                            throw new Exception($"unable to score {word}.");
                        }

                        if ((string)queryVector.Label != (string)hit.Node.Vector.Label)
                        {
                            throw new Exception($"best match was {hit.Node.Vector.Label} when searching for {word}.");
                        }

                        Debug.WriteLine($"{word} matched with {hit.Node.Vector.Label} with {hit.Score * 100}% certainty.");
                    }
                }
            });
        }

        [Test]
        public void Can_traverse_streamed()
        {
            var model = new BagOfCharsModel();

            var index = model.CreateTree(new LogStructuredIndexingStrategy(model), _data);
            var collectionId = (GetType() + "Can_traverse_streamed").ToHash();

            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var postingsStream = new MemoryStream())
            using (var postingsIndexStream = new MemoryStream())
            using (var pageIndexStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter(indexStream, keepStreamOpen: true))
                using (var postingsWriter = new PostingsWriter(postingsStream, new PostingsIndexAppender(postingsIndexStream), new PostingsIndexUpdater(postingsIndexStream), new PostingsIndexReader(postingsIndexStream), keepOpen:true))
                {
                    writer.CreatePage(index, vectorStream, postingsWriter, new PageIndexWriter(pageIndexStream, keepStreamOpen: true), new Dictionary<(long keyId, long pageId), HashSet<long>>());
                }

                indexStream.Flush();
                vectorStream.Flush();
                postingsStream.Flush();
                postingsIndexStream.Flush();
                pageIndexStream.Flush();

                vectorStream.Position = 0;
                indexStream.Position = 0;
                postingsStream.Position = 0;
                postingsIndexStream.Position = 0;
                pageIndexStream.Position = 0;

                Assert.DoesNotThrow(() =>
                {
                    using (var pageIndexReader = new PageIndexReader(pageIndexStream))
                    using (var columnReader = new ColumnReader(pageIndexReader.ReadAll(), indexStream, vectorStream, model))
                    using(var postingsReader = new PostingsReader(postingsStream, new PostingsIndexReader(postingsIndexStream), collectionId))
                    {
                        var docId = 0;

                        foreach (var word in _data)
                        {
                            foreach (var queryVector in model.CreateEmbedding(word, true))
                            {
                                var hit = columnReader.ClosestMatchOrNullStoppingAtFirstIdenticalPage(queryVector);

                                if (hit == null)
                                {
                                    throw new Exception($"unable to find {word} in tree.");
                                }

                                if (hit.Score < model.IdenticalAngle)
                                {
                                    throw new Exception($"unable to score {word}.");
                                }

                                var found = false;
                                foreach (var posting in postingsReader.Read(0, hit.PostingsPageIds))
                                {
                                    if (docId == posting.docId)
                                    {
                                        found = true;
                                        break;
                                    }
                                }

                                if (!found)
                                {
                                    throw new Exception($"unable to find {queryVector.Label}.");
                                }


                                Debug.WriteLine($"{word} matched vector in disk with {hit.Score * 100}% certainty.");
                            }

                            docId++;
                        }
                    }
                });
            }
        }

        [Test]
        public void Can_traverse_streamed_paged()
        {
            var model = new BagOfCharsModel();

            const int numOfPages = 2;

            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var postingsStream = new MemoryStream())
            using (var appendableIndexStream = new MemoryStream())
            using (var seekableIndexStream = new MemoryStream())
            using (var postingsIndex = new MemoryStream())
            using (var pageStream = new MemoryStream())
            {
                var batchSize = (int)Math.Ceiling((double)_data.Length / numOfPages);

                foreach (var batch in _data.Batch(batchSize))
                {
                    var index = model.CreateTree(new LogStructuredIndexingStrategy(model), batch.ToArray());

                    using (var writer = new ColumnWriter(indexStream, keepStreamOpen: true))
                    using (var postingsWriter = new PostingsWriter(postingsStream, new PostingsIndexAppender(appendableIndexStream), new PostingsIndexUpdater(seekableIndexStream), new PostingsIndexReader(postingsIndex), keepOpen: true))
                    {
                        writer.CreatePage(index, vectorStream, postingsWriter, new PageIndexWriter(pageStream, keepStreamOpen: true), new Dictionary<(long keyId, long pageId), HashSet<long>>());
                    }
                }

                pageStream.Position = 0;
                vectorStream.Position = 0;
                indexStream.Position = 0;
                postingsStream.Position = 0;
                appendableIndexStream.Position = 0;
                seekableIndexStream.Position = 0;
                postingsIndex.Position = 0;
                pageStream.Position = 0;

                Assert.DoesNotThrow(() =>
                {
                    using (var pageIndexReader = new PageIndexReader(pageStream))
                    using (var reader = new ColumnReader(pageIndexReader.ReadAll(), indexStream, vectorStream, model))
                    {
                        foreach (var word in _data)
                        {
                            foreach (var queryVector in model.CreateEmbedding(word, true))
                            {
                                var hit = reader.ClosestMatchOrNullScanningAllPages(queryVector);

                                if (hit == null)
                                {
                                    throw new Exception($"unable to find {word} in tree.");
                                }

                                if (hit.Score < model.IdenticalAngle)
                                {
                                    throw new Exception($"unable to score {word}.");
                                }

                                Debug.WriteLine($"{word} matched vector in disk with {hit.Score * 100}% certainty.");
                            }
                        }
                    }
                });
            }
        }

        [Test]
        public void Can_index_paged()
        {
            const int numOfPages = 2;
            const string fieldName = "title";
            var model = new BagOfCharsModel();
            var indexingStrategy = new LogStructuredIndexingStrategy(model);
            var keyRepository = new KeyRepository();
            ulong collectionId = "BagOfCharsModelTests_Can_index_paged".ToHash();
            using (var documentStream = new MemoryStream())
            using (var documentIndexStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var keyStream = new MemoryStream())
            using (var valueIndexStream = new MemoryStream())
            using (var keyIndexStream = new MemoryStream())
            using (var keyMapStream = new MemoryStream())
            using (var writeSession = new WriteSession(
                new DocumentWriter(
                    collectionId, 
                    documentStream, 
                    documentIndexStream, 
                    valueStream, 
                    keyStream, 
                    valueIndexStream, 
                    keyIndexStream, 
                    keyRepository)))
            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var postingsStream = new MemoryStream())
            using (var pageIndexStream = new MemoryStream())
            using (var postingsIndexReadStream = new MemoryStream())
            using (var postingsIndexUpdateStream = new MemoryStream())
            using (var postingsIndexAppendStream = new MemoryStream())
            using (var indexWriteStream = new IndexWriteStream(indexStream, vectorStream, postingsStream, pageIndexStream, postingsIndexReadStream, postingsIndexUpdateStream, postingsIndexAppendStream, keepOpen:true))
            {
                foreach (var item in _data)
                {
                    writeSession.Put(new Document(new Field[] { new Field(fieldName, item) }));
                }

                documentStream.Position = 0;
                documentIndexStream.Position = 0;
                valueStream.Position = 0;
                keyStream.Position = 0;
                valueIndexStream.Position = 0;
                keyIndexStream.Position = 0;
                keyMapStream.Position = 0;

                var documentReaders = new Dictionary<ulong, DocumentReader> { { collectionId, new DocumentReader(collectionId, valueStream, keyStream, documentStream, valueIndexStream, keyIndexStream, documentIndexStream) } };

                Assert.DoesNotThrow(() =>
                {
                    using (var documents = new DocumentStreamSession(keyRepository, documentReaders))
                    using (var indexSession = new IndexSession<string>(model, indexingStrategy, _sessionFactory, null, collectionId))
                    {
                        var keyId = keyRepository.GetKeyId(collectionId, fieldName.ToHash());

                        foreach (var page in documents.ReadDocuments(collectionId, new HashSet<string> { fieldName }).Batch(_data.Length/numOfPages))
                        {
                            foreach (var document in page)
                            {
                                foreach (var field in document.Fields)
                                {
                                    indexSession.Put(document.Id, field.KeyId, (string)field.Value, label: false);
                                }
                            }
                            indexSession.Commit(keyId, indexWriteStream);
                        }
                    }

                    indexStream.Position = 0;
                    vectorStream.Position = 0;
                    postingsStream.Position = 0;
                    pageIndexStream.Position = 0;
                    postingsIndexReadStream.Position = 0;
                    postingsIndexUpdateStream.Position = 0;
                    postingsIndexAppendStream.Position = 0;

                    using (var pageIndexReader = new PageIndexReader(pageIndexStream))
                    using (var reader = new ColumnReader(pageIndexReader.ReadAll(), indexStream, vectorStream, model))
                    {
                        foreach (var word in _data)
                        {
                            foreach (var queryVector in model.CreateEmbedding(word, true))
                            {
                                var hit = reader.ClosestMatchOrNullScanningAllPages(queryVector);

                                if (hit == null)
                                {
                                    throw new Exception($"unable to find {word} in tree.");
                                }

                                if (hit.Score < model.IdenticalAngle)
                                {
                                    throw new Exception($"unable to find {word}.");
                                }

                                Debug.WriteLine($"{word} matched vector in index with {hit.Score * 100}% certainty.");
                            }
                        }
                    }
                });
            }
        }

        [Test]
        public void Can_search_paged_overlapped()
        {
            const string fieldName = "title";
            var model = new BagOfCharsModel();
            var indexingStrategy = new LogStructuredIndexingStrategy(model);
            var keyRepository = new KeyRepository();
            ulong collectionId = "BagOfCharsModelTests_Can_index_paged".ToHash();
            var fieldsOfInterest = new HashSet<string> { fieldName };

            using (var documentStream = new MemoryStream())
            using (var documentIndexStream = new MemoryStream())
            using (var valueStream = new MemoryStream())
            using (var keyStream = new MemoryStream())
            using (var valueIndexStream = new MemoryStream())
            using (var keyIndexStream = new MemoryStream())
            using (var keyMapStream = new MemoryStream())
            using (var writeSession = new WriteSession(
                new DocumentWriter(
                    collectionId,
                    documentStream,
                    documentIndexStream,
                    valueStream,
                    keyStream,
                    valueIndexStream,
                    keyIndexStream,
                    keyRepository)))
            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var postingsStream = new MemoryStream())
            using (var pageIndexStream = new MemoryStream())
            using (var postingsIndexStream = new MemoryStream())
            using (var indexWriteStream = new IndexWriteStream(indexStream, vectorStream, postingsStream, pageIndexStream, postingsIndexStream, postingsIndexStream, postingsIndexStream, keepOpen: true))
            {
                foreach (var item in _data)
                {
                    writeSession.Put(new Document(new Field[] { new Field(fieldName, item) }));
                }

                documentStream.Flush();
                documentIndexStream.Flush();
                valueStream.Flush();
                keyStream.Flush();
                valueIndexStream.Flush();
                keyIndexStream.Flush();
                keyMapStream.Flush();

                documentStream.Position = 0;
                documentIndexStream.Position = 0;
                valueStream.Position = 0;
                keyStream.Position = 0;
                valueIndexStream.Position = 0;
                keyIndexStream.Position = 0;
                keyMapStream.Position = 0;

                var keyId = keyRepository.GetKeyId(collectionId, fieldName.ToHash());
                var documentReaders = new Dictionary<ulong, DocumentReader> { { collectionId, new DocumentReader(collectionId, valueStream, keyStream, documentStream, valueIndexStream, keyIndexStream, documentIndexStream) } };

                using (var documents = new DocumentStreamSession(keyRepository, documentReaders))
                using (var indexSession = new IndexSession<string>(model, indexingStrategy, _sessionFactory, null, collectionId))
                {
                    var storedDocs = documents.ReadDocuments(collectionId, fieldsOfInterest).ToList();

                    //foreach (var document in storedDocs.Take((storedDocs.Count / 2) - 1))
                    //{
                    //    foreach (var field in document.Fields)
                    //    {
                    //        indexSession.Put(document.Id, field.KeyId, (string)field.Value, label: false);
                    //    }
                    //}

                    foreach (var document in storedDocs)
                    {
                        foreach (var field in document.Fields)
                        {
                            indexSession.Put(document.Id, field.KeyId, (string)field.Value, label: false);
                        }
                    }

                    indexSession.Commit(keyId, indexWriteStream);
                }

                //documentStream.Position = 0;
                //documentIndexStream.Position = 0;
                //valueStream.Position = 0;
                //keyStream.Position = 0;
                //valueIndexStream.Position = 0;
                //keyIndexStream.Position = 0;
                //keyMapStream.Position = 0;

                //indexStream.Position = 0;
                //vectorStream.Position = 0;
                //postingsStream.Position = 0;
                //pageIndexStream.Position = 0;
                //postingsStream.Position = 0;

                //Assert.DoesNotThrow(() =>
                //{
                //    using (var documents = new DocumentStreamSession(keyRepository, documentReaders))
                //    using (var pageIndexReader = new PageIndexReader(pageIndexStream, keepOpen:true))
                //    using (var reader = new ColumnReader(pageIndexReader.ReadAll(), indexStream, vectorStream, model, keepOpen:true))
                //    {
                //        foreach (var document in documents.ReadDocuments(collectionId, fieldsOfInterest))
                //        {
                //            var word = (string)document.Get(fieldName).Value;

                //            foreach (var queryVector in model.CreateEmbedding(word, true))
                //            {
                //                var hit = reader.ClosestMatchOrNullScanningAllPages(queryVector);

                //                if (hit == null)
                //                {
                //                    throw new Exception($"unable to find {word} in tree.");
                //                }

                //                if (hit.Score < model.IdenticalAngle)
                //                {
                //                    throw new Exception($"unable to find {word}.");
                //                }

                //                Debug.WriteLine($"{word} matched vector in index with {hit.Score * 100}% certainty.");
                //            }
                //        }
                //    }
                //});

                documentStream.Position = 0;
                documentIndexStream.Position = 0;
                valueStream.Position = 0;
                keyStream.Position = 0;
                valueIndexStream.Position = 0;
                keyIndexStream.Position = 0;
                keyMapStream.Position = 0;

                indexStream.Flush();
                vectorStream.Flush();
                postingsStream.Flush();
                pageIndexStream.Flush();
                postingsIndexStream.Flush();

                indexStream.Position = 0;
                vectorStream.Position = 0;
                postingsStream.Position = 0;
                pageIndexStream.Position = 0;
                postingsIndexStream.Position = 0;

                var postingsReaders = new Dictionary<(string, ulong, long), PostingsReader> { { ("", collectionId, keyId), new PostingsReader(postingsStream, new PostingsIndexReader(postingsIndexStream), collectionId) } };
                var columnReaders = new Dictionary<(string, ulong, long), ColumnReader> { { ("", collectionId, keyId), new ColumnReader(new PageIndexReader(pageIndexStream).ReadAll(), indexStream, vectorStream, model) } };

                using (var postingsResolver = new PostingsResolver(postingsReaders))
                using (var validateSession = new ValidateSession<string>(
                    collectionId,
                    new SearchSession(keyRepository, documentReaders, _sessionFactory, model, indexingStrategy, postingsResolver, columnReaders),
                    new QueryParser<string>("", keyRepository, model)))
                {
                    using (var documents = new DocumentStreamSession(keyRepository, documentReaders))
                    {
                        foreach (var doc in documents.ReadDocuments(collectionId, fieldsOfInterest))
                        {
                            Assert.DoesNotThrow(() =>
                            {
                                validateSession.Validate(doc);
                            });
                        }
                    }
                }
            }
        }

        [Test]
        public void Can_tokenize()
        {
            var model = new BagOfCharsModel();

            foreach (var data in _data)
            {
                var tokens = model.CreateEmbedding(data, true).ToList();
                var labels = tokens.Select(x => x.Label.ToString()).ToList();

                foreach( var token in tokens)
                {
                    Assert.IsTrue(labels.Contains(token.Label));
                }
            }
        }

        [SetUp]
        public void Setup()
        {
            _loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddDebug();
            });

            _sessionFactory = new SessionFactory(logger: _loggerFactory.CreateLogger<SessionFactory>());
        }

        [TearDown]
        public void TearDown()
        {
            _sessionFactory.Dispose();
        }
    }
}