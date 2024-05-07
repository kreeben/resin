using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// Write a collection of indices to disk.
    /// </summary>
    public class IndexWriter : IDisposable
    {
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;

        public IndexWriter(
            string directory,
            ulong collectionId,
            ILogger logger = null)
        {
            _directory = directory;
            _collectionId = collectionId;
            _logger = logger;
        }

        public void Dispose()
        {
        }

        public void Commit(IDictionary<long, VectorNode> index, IIndexReadWriteStrategy indexingStrategy)
        {
            foreach (var column in index)
            {
                indexingStrategy.Commit(_directory, _collectionId, column.Key, column.Value);
            }
        }
    }
}