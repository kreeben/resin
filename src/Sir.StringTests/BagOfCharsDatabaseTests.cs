using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Documents;
using Sir.Strings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sir.StringTests
{
    public class BagOfCharsDatabaseTests
    {
        private ILoggerFactory _loggerFactory;
        private string _directory = Path.Combine(Environment.CurrentDirectory, "testdata");
        private readonly string[] _data = ["Ferriman–Gallwey score", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope"];

        [Test]
        public void Can_stream_documents()
        {
            var model = new BagOfCharsModel();
            var strat = new LogStructuredIndexingStrategy(model);
            var collectionId = "Can_stream_documents".ToHash();
            var documents = _data.Select(x => new Document(new Field[] {new Field("title", x)})).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strat, _loggerFactory.CreateLogger("Debug")))
            {
                database.Truncate();

                foreach (var document in documents)
                {
                    database.Write(document, index: false);
                }

                database.Commit();

                var i = 0;

                foreach (var document in database.StreamDocuments(new HashSet<string> { "title" }, 0, int.MaxValue))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var documentWas = document.Get("title").Value;
                        var documentShouldBe = _data[i++];

                        if (!documentShouldBe.Equals(documentWas))
                        {
                            throw new Exception($"documentShouldBe: {documentShouldBe} documentWas: {documentWas} ");
                        }
                    });
                }
            }
        }

        [Test]
        public void Can_read()
        {
        }

        [Test]
        public void Can_write()
        {
        }

        [Test]
        public void Can_optimize_index()
        {
        }

        [SetUp]
        public void Setup()
        {
            _loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    //.AddFilter("Microsoft", LogLevel.Warning)
                    //.AddFilter("System", LogLevel.Warning)
                    .AddDebug();
            });

        }

        [TearDown]
        public void TearDown()
        {
        }
    }
}