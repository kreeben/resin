using Sir.Documents;
using System;

namespace Sir
{
    /// <summary>
    /// Validate that documents in a collection can be found by all of their terms.
    /// </summary>
    public class ValidateSession<T> : IDisposable
    {
        public ulong CollectionId { get; }

        private readonly SearchSession _readSession;
        private readonly QueryParser<T> _queryParser;

        public ValidateSession(
            ulong collectionId,
            SearchSession searchSession,
            QueryParser<T> queryParser
            )
        {
            CollectionId = collectionId;
            _readSession = searchSession;
            _queryParser = queryParser;
        }

        public void Validate(Document doc)
        {
            foreach (var field in doc.Fields)
            {
                if (string.IsNullOrWhiteSpace(field.Value.ToString()))
                    throw new ArgumentNullException(nameof(field));

                var query = _queryParser.Parse(CollectionId, (T)field.Value, field.Name, field.Name, true, false, true);
                const int take = 100000;
                var result = _readSession.SearchIdentical(query, take);
                bool isMatch = false;

                foreach (var document in result.Documents)
                {
                    if (doc.Id == document.Id)
                    {
                        isMatch = true;
                        break;
                    }
                }
                
                if (!isMatch)
                {
                    if (result.Total == 0)
                    {
                        throw new Exception($"unable to validate doc.Id {doc.Id} because no documents were found. field value: {field.Value}");
                    }
                    else if (result.Total == take)
                    {
                        throw new Exception($"unable to validate doc.Id {doc.Id} because page size {take} was too small. field value: {field.Value}");
                    }
                    else
                    {
                        throw new Exception($"unable to validate doc.Id {doc.Id} because wrong document was found. field value: {field.Value}");
                    }
                }
            }
        }

        public void Dispose()
        {
            _readSession.Dispose();
        }
    }
}