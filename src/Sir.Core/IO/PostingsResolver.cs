using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Sir.IO
{
    public class PostingsResolver : IDisposable
    {
        private readonly Dictionary<(string, ulong, long), PostingsReader> _readers = new Dictionary<(string, ulong, long), PostingsReader>();

        public PostingsResolver() { }

        public PostingsResolver(Dictionary<(string, ulong, long), PostingsReader> readers)
        {
            _readers = readers;
        }

        /// <summary>
        /// Read posting list document IDs into memory.
        /// </summary>
        public void Resolve(IQuery query, ISessionFactory sessionFactory, ILogger logger = null)
        {
            foreach (var term in query.AllTerms())
            {
                if (term.PostingsPageIds == null)
                    continue;

                PostingsReader reader;
                var key = (term.Directory, term.CollectionId, term.KeyId);

                if (!_readers.TryGetValue(key, out reader))
                {
                    reader = new PostingsReader(term.Directory, term.CollectionId, term.KeyId, sessionFactory, logger);

                    if (reader != null)
                    {
                        _readers.Add(key, reader);
                    }
                }

                if (reader != null)
                    term.DocumentIds = reader.Read(term.KeyId, term.PostingsPageIds);
            }
        }

        public void Dispose()
        {
            foreach (var reader in _readers.Values)
                reader.Dispose();
        }
    }
}