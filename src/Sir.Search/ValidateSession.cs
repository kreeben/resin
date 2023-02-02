using System;
using System.Linq;

namespace Sir
{
    /// <summary>
    /// Validate a collection.
    /// </summary>
    public class ValidateSession<T> : IDisposable
    {
        public ulong CollectionId { get; }

        private readonly SearchSession _searchSession;
        private readonly QueryParser<T> _queryParser;

        public ValidateSession(
            ulong collectionId,
            SearchSession searchSession,
            QueryParser<T> queryParser
            )
        {
            CollectionId = collectionId;
            _searchSession = searchSession;
            _queryParser = queryParser;
        }

        public void Validate(Document document, string fieldName)
        {
            foreach (var field in document.Fields)
            {
                if (fieldName == field.Name)
                {
                    if (string.IsNullOrWhiteSpace(field.Value.ToString()))
                        continue;

                    var query = _queryParser.Parse(CollectionId, (T)field.Value, field.Name, field.Name, true, false, true);
                    var result = _searchSession.SearchIdentical(query, 1000);
                    bool isMatch = false;
                    Document mostRecentDoc = null;

                    foreach (var d in result.Documents)
                    {
                        mostRecentDoc = d;

                        if (document.Id == d.Id)
                        {
                            isMatch = true;
                            break;
                        }
                    }

                    if (!isMatch)
                    {
                        if (result.Total == 0)
                        {
                            throw new Exception($"unable to validate doc.Id {document.Id} because no documents were found with {field.Name}: {field.Value}");
                        }
                        else
                        {
                            throw new Exception($"unable to validate doc.Id {document.Id} because wrong document was found. query: {field.Name}: {field.Value}. best hit: {field.Name}: {mostRecentDoc.Get(field.Name).Value}");
                        }
                    }
                }
            }
        }

        public void Dispose()
        {
            _searchSession.Dispose();
        }
    }
}