using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Images;
using Sir.IO;
using Sir.Mnist;
using System;
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