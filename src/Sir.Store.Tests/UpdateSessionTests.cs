using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Documents;
using Sir.Search;
using System;
using System.Diagnostics;
using System.Linq;

namespace Sir.Tests
{
    public class UpdateSessionTests
    {
        private ILoggerFactory _loggerFactory;
        private Database _sessionFactory;
        private string _directory = @"c:\temp\sir_tests";

        private readonly string[] _data = new string[] { "apple", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope" };

        [Test]
        public void Can_update_string_field()
        {
            var model = new BagOfCharsModel();
            const string collection = "Can_update";
            var collectionId = collection.ToHash();
            const string fieldName = "description";
            const string updatedWord = "xylophone";

            for (int documentIdToUpdate = 0; documentIdToUpdate < _data.Length; documentIdToUpdate++)
            {
                _sessionFactory.Truncate(_directory, collectionId);

                using (var stream = new WritableIndexStream(_directory, collectionId, _sessionFactory))
                using (var writeSession = new WriteSession(new DocumentWriter(_directory, collectionId, _sessionFactory)))
                {
                    var keyId = writeSession.EnsureKeyExists(fieldName);

                    for (long i = 0; i < _data.Length; i++)
                    {
                        var data = _data[i];

                        using (var indexSession = new IndexSession<string>(model, model))
                        {
                            var doc = new Document(new Field[] { new Field(fieldName, data) });

                            writeSession.Put(doc);
                            indexSession.Put(doc.Id, keyId, data);
                            stream.Persist(indexSession.InMemoryIndices());
                        }
                    }
                }

                var queryParser = new QueryParser<string>(_directory, _sessionFactory, model);

                using (var searchSession = new SearchSession(_directory, _sessionFactory, model, _loggerFactory.CreateLogger<SearchSession>()))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        foreach (var word in _data)
                        {
                            var query = queryParser.Parse(collection, word, fieldName, fieldName, and: true, or: false);
                            var result = searchSession.Search(query, 0, 1);
                            var document = result.Documents.FirstOrDefault();

                            if (document == null)
                            {
                                throw new Exception($"unable to find {word}.");
                            }

                            if (document.Score < model.IdenticalAngle)
                            {
                                throw new Exception($"unable to score {word}.");
                            }

                            Debug.WriteLine($"{word} matched with {document.Score * 100}% certainty.");
                        }
                    });
                }

                using (var updateSession = new UpdateSession(_directory, collectionId, _sessionFactory))
                {
                    updateSession.Update(documentIdToUpdate, 0, updatedWord);
                }

                using (var searchSession = new SearchSession(_directory, _sessionFactory, model, _loggerFactory.CreateLogger<SearchSession>()))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var count = 0;

                        foreach (var word in _data)
                        {
                            var query = queryParser.Parse(collection, word, fieldName, fieldName, and: true, or: false);
                            var result = searchSession.Search(query, 0, 1);
                            var document = result.Documents.FirstOrDefault();

                            if (document == null)
                            {
                                throw new Exception($"unable to find {word}.");
                            }

                            if (document.Score < model.IdenticalAngle)
                            {
                                throw new Exception($"unable to score {word}.");
                            }

                            Debug.WriteLine($"{word} matched with {document.Score * 100}% certainty.");

                            if (count++ == documentIdToUpdate)
                            {
                                continue;
                            }
                        }
                    });
                    var r = searchSession.Search(queryParser.Parse(collection, _data[documentIdToUpdate], fieldName, fieldName, and: true, or: false), 0, 1);
                    Assert.IsTrue(updatedWord == searchSession.Search(queryParser.Parse(collection, _data[documentIdToUpdate], fieldName, fieldName, and: true, or: false), 0, 1).Documents.First().Fields.First().Value.ToString());
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

            _sessionFactory = new Database(logger: _loggerFactory.CreateLogger<Database>());
        }

        [TearDown]
        public void TearDown()
        {
            _sessionFactory.Dispose();
        }
    }
}