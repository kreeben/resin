using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Documents;
using Sir.Images;
using Sir.IO;
using Sir.Mnist;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sir.ImageTests
{
    public class ImageModelTests
    {
        private ILoggerFactory _loggerFactory;
        private SessionFactory _sessionFactory;

        private readonly IImage[] _data = new MnistReader(
                @"resources\t10k-images.idx3-ubyte",
                @"resources\t10k-labels.idx1-ubyte").Read().Take(100).ToArray();

        [Test]
        public void Can_traverse_index_in_memory()
        {
            // Use the same set of images to both create and validate a linear classifier.

            var model = new LinearClassifierImageModel();

            var index = model.CreateTree(new LogStructuredIndexingStrategy(model), _data);

            Print(index);

            Assert.DoesNotThrow(() =>
            {
                var count = 0;
                var errors = 0;

                foreach (var image in _data)
                {
                    foreach (var queryVector in model.CreateEmbedding(image, true))
                    {
                        var hit = PathFinder.ClosestMatch(index, queryVector, model);

                        if (hit == null)
                        {
                            throw new Exception($"unable to find {image} in index.");
                        }

                        if (!hit.Node.Vector.Label.Equals(image.Label))
                        {
                            errors++;
                        }

                        Debug.WriteLine($"{image} matched with {hit.Node.Vector.Label} with {hit.Score * 100}% certainty.");

                        count++;
                    }
                }

                var errorRate = (float)errors / count;

                if (errorRate > 0)
                {
                    throw new Exception($"error rate: {errorRate * 100}%. too many errors.");
                }

                Debug.WriteLine($"error rate: {errorRate}");
            });
        }

        [Test]
        public void Can_traverse_streamed()
        {
            // Use the same set of images to both create and validate a linear classifier.

            var model = new LinearClassifierImageModel();

            var index = model.CreateTree(new LogStructuredIndexingStrategy(model), _data);

            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var postingsStream = new MemoryStream())
            using (var appendableIndexStream = new MemoryStream())
            using (var seekableIndexStream = new MemoryStream())
            using (var postingsIndex = new MemoryStream())
            using (var pageStream = new MemoryStream())
            {
                using (var writer = new ColumnWriter(indexStream, keepStreamOpen: true))
                using (var postingsWriter = new PostingsWriter(postingsStream, new PostingsIndexAppender(appendableIndexStream), new PostingsIndexUpdater(seekableIndexStream), new PostingsIndexReader(postingsIndex), keepOpen: true))
                {
                    writer.CreatePage(index, vectorStream, postingsWriter, new PageIndexWriter(pageStream, keepStreamOpen: true), new Dictionary<(long keyId, long pageId), HashSet<long>>());
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
        public void Can_traverse_streamed_paged()
        {
            var model = new LinearClassifierImageModel();

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
            var model = new LinearClassifierImageModel();
            var indexingStrategy = new LogStructuredIndexingStrategy(model);
            var keyRepository = new KeyRepository();
            ulong collectionId = "ImageModelTests_Can_index_paged".ToHash();
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
                    using (var indexSession = new IndexSession<IImage>(model, indexingStrategy, _sessionFactory, null, collectionId))
                    {
                        var keyId = keyRepository.GetKeyId(collectionId, fieldName.ToHash());

                        foreach (var page in documents.ReadDocuments(collectionId, new HashSet<string> { fieldName }).Batch(_data.Length / numOfPages))
                        {
                            foreach (var document in page)
                            {
                                foreach (var field in document.Fields)
                                {
                                    indexSession.Put(document.Id, field.KeyId, new MnistImage((byte[])field.Value, 0), label: false);
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
            var trainingData = new MnistReader(
                @"resources\t10k-images.idx3-ubyte",
                @"resources\t10k-labels.idx1-ubyte").Read().Take(100).ToArray();

            var model = new LinearClassifierImageModel();

            foreach (var data in trainingData)
            {
                var tokens = model.CreateEmbedding(data, true).ToList();
                var labels = tokens.Select(x => x.Label.ToString()).ToList();

                foreach (var token in tokens)
                {
                    Assert.IsTrue(labels.Contains(token.Label));
                }
            }
        }

        private static void Print(VectorNode tree)
        {
            var diagram = PathFinder.Visualize(tree);
            File.WriteAllText("imagemodeltesttree.txt", diagram);
            Debug.WriteLine(diagram);
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
    }
}