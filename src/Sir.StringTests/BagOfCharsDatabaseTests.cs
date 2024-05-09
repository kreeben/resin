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
        public void Can_stream()
        {
            var model = new BagOfCharsModel();
            var strategy = new LogStructuredIndexingStrategy(model);
            var collectionId = "BagOfCharsDatabaseTests.Can_stream".ToHash();
            var documents = _data.Select(x => new Document(new Field[] {new Field("title", x)})).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy, _loggerFactory.CreateLogger("Debug")))
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
        public void Can_read_and_write()
        {
            var model = new BagOfCharsModel();
            var strategy = new LogStructuredIndexingStrategy(model);
            var collectionId = "BagOfCharsDatabaseTests.Can_read_and_write".ToHash();
            var documents = _data.Select(x => new Document(new Field[] { new Field("title", x) })).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy, _loggerFactory.CreateLogger("Debug")))
            {
                database.Truncate();

                foreach (var document in documents)
                {
                    database.Write(document);
                }

                database.Commit();

                var queryParser = database.CreateQueryParser();

                foreach (var word in _data)
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var query = queryParser.Parse(collectionId, word, "title", "title", and:true, or:false, label:true);
                        var result = database.Read(query, skip: 0, take: 1);

                        var documentWas = result.Documents.First().Get("title").Value;
                        var documentShouldBe = word;

                        if (!documentShouldBe.Equals(documentWas))
                        {
                            throw new Exception($"documentShouldBe: {documentShouldBe} documentWas: {documentWas} ");
                        }
                    });
                }
            }
        }

        [Test]
        public void Can_optimize_index()
        {
            var model = new BagOfCharsModel();
            var strategy = new LogStructuredIndexingStrategy(model);
            var collectionId = "BagOfCharsDatabaseTests.Can_optimize_index".ToHash();
            var documents = _data.Select(x => new Document(new Field[] { new Field("title", x) })).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy, _loggerFactory.CreateLogger("Debug")))
            {
                database.Truncate();

                foreach (var document in documents)
                {
                    database.Write(document, store:true, index:false); // note: no indexing going on here
                }

                database.Commit();

                var queryParser = database.CreateQueryParser();

                foreach (var word in _data)
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var query = queryParser.Parse(collectionId, word, "title", "title", and: true, or: false, label: true);
                        var result = database.Read(query, skip: 0, take: 1);

                        if (result.Count > 0)
                            throw new Exception("For unknown reasons we can search the index without having created it, which is very, very strange, so this should never happen.");
                    });
                }

                database.OptimizeAllIndices();

                foreach (var word in _data)
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var query = queryParser.Parse(collectionId, word, "title", "title", and: true, or: false, label: true);
                        var result = database.Read(query, skip: 0, take: 1);

                        var documentWas = result.Documents.First().Get("title").Value;
                        var documentShouldBe = word;

                        if (!documentShouldBe.Equals(documentWas))
                        {
                            throw new Exception($"documentShouldBe: {documentShouldBe} documentWas: {documentWas} ");
                        }
                    });
                }
            }
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