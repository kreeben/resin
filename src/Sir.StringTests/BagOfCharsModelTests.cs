using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.IO;
using Sir.Strings;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sir.StringTests
{
    public class BagOfCharsModelTests
    {
        private ILoggerFactory _loggerFactory;

        private readonly string[] _data = new string[] { "Ferriman–Gallwey score", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope" };

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

                Assert.DoesNotThrow(() =>
                {
                    using (var pageIndexReader = new PageIndexReader(pageStream))
                    using (var reader = new ColumnReader(pageIndexReader.ReadAll(), new StreamPageReader(indexStream, vectorStream)))
                    {
                        foreach (var word in _data)
                        {
                            foreach (var queryVector in model.CreateEmbedding(word, true))
                            {
                                var hit = reader.ClosestMatchOrNullScanningAllPages(queryVector, model);

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

        }

        [TearDown]
        public void TearDown()
        {
        }
    }
}