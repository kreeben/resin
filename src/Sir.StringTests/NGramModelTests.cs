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
    public class NGramModelTests
    {
        private ILoggerFactory _loggerFactory;
        private SessionFactory _sessionFactory;

        private readonly string[] _data = new string[] { "I would like an apple.", "apples are sour", "that's an apricote", "apricots are sweet", "a green avocado", "there are many avocados", "here's a banana", "I like bananas because they are yellow", "one blueberry fell on the floor", "blueberries all over the kitcheh", "cantalope" };

        [Test]
        public void Can_traverse_index_in_memory()
        {
            var model = new NGramModel(new BagOfCharsModel());

            var index = model.CreateTree(new LogStructuredIndexingStrategy(model), _data);

            Debug.WriteLine(PathFinder.Visualize(index));

            Assert.DoesNotThrow(() =>
            {
                foreach (var phrase in _data)
                {
                    foreach (var queryVector in model.CreateEmbedding(phrase, true))
                    {
                        var hit = PathFinder.ClosestMatch(index, queryVector, model);

                        if (hit == null)
                        {
                            throw new Exception($"unable to find {phrase} in index.");
                        }

                        if (hit.Score < model.IdenticalAngle)
                        {
                            throw new Exception($"unable to score {phrase}.");
                        }

                        Debug.WriteLine($"{phrase} matched with {hit.Node.Vector.Label} with {hit.Score * 100}% certainty.");
                    }
                }
            });
        }

        [Test]
        public void Can_traverse_streamed()
        {
            //var model = new NGramModel(new BagOfCharsModel());

            //var index = model.CreateTree(new LogStructuredIndexingStrategy(model), _data);

            //using (var indexStream = new MemoryStream())
            //using (var vectorStream = new MemoryStream())
            //using (var pageStream = new MemoryStream())
            //{
            //    using (var writer = new ColumnWriter(indexStream, keepStreamOpen: true))
            //    {
            //        writer.CreatePage(index, vectorStream, new PageIndexWriter(pageStream, keepStreamOpen: true));
            //    }

            //    pageStream.Position = 0;

            //    Assert.DoesNotThrow(() =>
            //    {
            //        using (var pageIndexReader = new PageIndexReader(pageStream))
            //        using (var reader = new ColumnReader(pageIndexReader.ReadAll(), indexStream, vectorStream))
            //        {
            //            foreach (var word in _data)
            //            {
            //                foreach (var queryVector in model.CreateEmbedding(word, true))
            //                {
            //                    var hit = reader.ClosestMatchOrNullScanningAllPages(queryVector, model);

            //                    if (hit == null)
            //                    {
            //                        throw new Exception($"unable to find \"{word}\" in tree.");
            //                    }

            //                    if (hit.Score < model.IdenticalAngle)
            //                    {
            //                        throw new Exception($"unable to score \"{word}\".");
            //                    }

            //                    Debug.WriteLine($"\"{word}\" matched vector in disk with {hit.Score * 100}% certainty.");
            //                }
            //            }
            //        }
            //    });
            //}
        }

        [Test]
        public void Can_tokenize()
        {
            var model = new NGramModel(new BagOfCharsModel());

            foreach (var data in _data)
            {
                var tokens = model.CreateEmbedding(data, true).ToList();
                var labels = tokens.Select(x => x.Label.ToString()).ToList();

                foreach (var token in tokens)
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