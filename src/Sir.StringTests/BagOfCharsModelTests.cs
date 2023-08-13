using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Documents;
using Sir.IO;
using Sir.Strings;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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

            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var pageStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter(indexStream, keepStreamOpen: true))
                {
                    writer.CreatePage(index, vectorStream, new PageIndexWriter(pageStream, keepStreamOpen: true));
                }

                pageStream.Position = 0;
                vectorStream.Position = 0;
                indexStream.Position = 0;

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
        public void Can_traverse_streamed_paged()
        {
            const int numOfPages = 2;
            var model = new BagOfCharsModel();

            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var pageStream = new MemoryStream())
            using (var writer = new ColumnWriter(indexStream))
            {
                foreach (var batch in _data.Batch((int)Math.Ceiling((double)_data.Length / numOfPages)))
                {
                    var index = model.CreateTree(new LogStructuredIndexingStrategy(model), batch.ToArray());

                    writer.CreatePage(index, vectorStream, new PageIndexWriter(pageStream, keepStreamOpen: true));
                }

                pageStream.Position = 0;
                vectorStream.Position = 0;
                indexStream.Position = 0;

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
            using (var indexWriteStream = new IndexWriteStream(indexStream, vectorStream, postingsStream, pageIndexStream, postingsIndexReadStream, postingsIndexUpdateStream, postingsIndexAppendStream))
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