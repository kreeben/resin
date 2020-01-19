using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sir.Search
{
    /// <summary>
    /// Validate a collection.
    /// </summary>
    public class ValidateSession : IDisposable
    {
        public ulong CollectionId { get; }

        private readonly SessionFactory _sessionFactory;
        private readonly IConfigurationProvider _config;
        private readonly IStringModel _model;
        private readonly IReadSession _readSession;
        private readonly ILogger _logger;

        public ValidateSession(
            ulong collectionId,
            SessionFactory sessionFactory, 
            IStringModel model,
            IConfigurationProvider config,
            ILogger logger
            )
        {
            CollectionId = collectionId;
            _sessionFactory = sessionFactory;
            _config = config;
            _model = model;
            _readSession = sessionFactory.CreateReadSession();
            _logger = logger;
        }

        public void Validate(IDictionary<string, object> doc, params string[] validateFields)
        {
            var docId = (long)doc["___docid"];

            foreach(var key in validateFields)
            {
                object obj;

                if (doc.TryGetValue(key, out obj))
                {
                    var value = (string)obj;
                    var keyId = _sessionFactory.GetKeyId(CollectionId, key.ToHash());
                    var query = new Query(
                        _model.Tokenize(value.ToCharArray())
                            .Select(x => new Term(CollectionId, keyId, key, x, and: true, or: false, not: false)).ToList(),
                        and: true,
                        or: false,
                        not: false);

                    _readSession.EnsureIsValid(query, docId);

                    _logger.LogInformation($"validated {docId}");
                }
            }
        }

        public void Dispose()
        {
            _readSession.Dispose();
        }
    }

    public class DocumentComparer : IEqualityComparer<IDictionary<string, object>>
    {
        public bool Equals(IDictionary<string, object> x, IDictionary<string, object> y)
        {
            return (long)x["___docid"] == (long)y["___docid"];
        }

        public int GetHashCode(IDictionary<string, object> obj)
        {
            return obj["___docid"].GetHashCode();
        }
    }
}