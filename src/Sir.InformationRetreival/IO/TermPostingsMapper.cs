using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Sir.IO
{
    /// <summary>
    /// Read postings from storage and map them to query terms.
    /// </summary>
    public class TermPostingsMapper : IDisposable
    {
        private readonly Dictionary<(string directory, ulong collectionId, long keyId), PostingsReader> _readers = new Dictionary<(string, ulong, long), PostingsReader>();
        private readonly ILogger _logger;

        public TermPostingsMapper(ILogger logger = null)
        {
            _logger = logger;
        }

        public void ReadAndMap(Query query)
        {
            foreach (var term in query.AllTerms())
            {
                if (term.PostingsOffsets == null)
                    continue;

                PostingsReader reader;
                var key = (term.Directory, term.CollectionId, term.KeyId);

                if (!_readers.TryGetValue(key, out reader))
                {
                    reader = new PostingsReader(term.Directory, term.CollectionId, term.KeyId, _logger);
                    _readers.Add(key, reader);
                }

                if (reader != null)
                    term.DocumentIds = reader.Read(term.KeyId, term.PostingsOffsets);
            }
        }

        public void Dispose()
        {
            foreach (var reader in _readers.Values)
                reader.Dispose();
        }
    }
}