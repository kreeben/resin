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
        private readonly string[] _dataPage0 = ["Ferriman–Gallwey score", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope"];
        private readonly string[] _dataPage1 = ["score", "apples and teddybears", "apricote sauce", "hey baberibba", "avocado sundae", "avocados are nice", "banana split", "I'm going bananas", "blueberry pie", "blueberries and sauce", "cantalope"];

        [Test]
        public void Can_stream()
        {
            var model = new BagOfCharsModel();
            var strategy = new LogStructuredIndexingStrategy(model);
            var collectionId = "BagOfCharsDatabaseTests.Can_stream".ToHash();
            var documents = _dataPage0.Select(x => new Document(new Field[] {new Field("title", x)})).ToList();

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
                        var documentShouldBe = _dataPage0[i++];

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
            var documents = _dataPage0.Select(x => new Document(new Field[] { new Field("title", x) })).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy, _loggerFactory.CreateLogger("Debug")))
            {
                database.Truncate();

                foreach (var document in documents)
                {
                    database.Write(document);
                }

                database.Commit();

                var queryParser = database.CreateQueryParser();

                foreach (var word in _dataPage0)
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
        public void Can_read_and_write_paged()
        {
            // This test fails because BagOfCharsModel is also a bag of words model thus it scores 1 if there's a hit in a phrase that's not identical to the term.
            var model = new BagOfCharsModel();
            var strategy = new LogStructuredIndexingStrategy(model);
            var collectionId = "BagOfCharsDatabaseTests.Can_read_and_write_paged".ToHash();
            var page0 = _dataPage0.Select(x => new Document(new Field[] { new Field("title", x) })).ToList();
            var page1 = _dataPage1.Select(x => new Document(new Field[] { new Field("title", x) })).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy, _loggerFactory.CreateLogger("Debug")))
            {
                database.Truncate();

                foreach (var document in page0)
                {
                    database.Write(document, label: true);
                }

                database.Commit(); // create page

                foreach (var document in page1)
                {
                    database.Write(document, label:true);
                }

                database.Commit(); // create another page

                var queryParser = database.CreateQueryParser();

                foreach (var word in _dataPage0)
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

                foreach (var word in _dataPage1)
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var query = queryParser.Parse(collectionId, word, "title", "title", and: true, or: false, label: true);
                        var result = database.Read(query, skip: 0, take: 2);

                        var documentWas = result.Documents.First().Get("title").Value;
                        var documentShouldBe = word;

                        if (!documentShouldBe.Equals(documentWas))
                        {
                            throw new Exception($"documentShouldBe: {documentShouldBe} documentWas: {documentWas} ");
                        }

                        //var found = false;

                        //foreach(var document in result.Documents)
                        //{
                        //    var documentWas = result.Documents.First().Get("title").Value;
                        //    var documentShouldBe = word;

                        //    if (documentShouldBe.Equals(documentWas))
                        //    {
                        //        found = true;
                        //        break;
                        //    }
                        //}

                        //if (!found)
                        //    throw new Exception($"document not found. documentShouldBe: {word} ");

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
            var documents = _dataPage0.Select(x => new Document(new Field[] { new Field("title", x) })).ToList();

            using (var database = new DocumentDatabase<string>(_directory, collectionId, model, strategy, _loggerFactory.CreateLogger("Debug")))
            {
                database.Truncate();

                foreach (var document in documents)
                {
                    database.Write(document, store:true, index:false); // note: no indexing going on here
                }

                database.Commit();

                var queryParser = database.CreateQueryParser();

                foreach (var word in _dataPage0)
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

                foreach (var word in _dataPage0)
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