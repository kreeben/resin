using Microsoft.Extensions.Logging;
using Sir.Documents;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.IO
{
    /// <summary>
    /// Allocate postings in memory.
    /// </summary>
    public class PostingsReader : IDisposable
    {
        private readonly Stream _stream;
        private readonly ILogger _logger;
        private readonly ulong _collectionId;
        private readonly DocumentStreamSession _documentSession;

        public PostingsReader(string directory, ulong collectionId, long keyId, ILogger logger = null)
        {
            _collectionId = collectionId;
            _stream = DocumentRegistryReader.CreateReadStream(Path.Combine(directory, $"{Database.GetIndexCollectionId(_collectionId)}.{keyId}.pos"));
            _logger = logger;
            _documentSession = new DocumentStreamSession(directory);
        }

        public HashSet<(ulong, long)> Read(long keyId, IList<long> offsets)
        {
            var time = Stopwatch.StartNew();
            var documents = new HashSet<(ulong, long)>(); // collection ID, document ID

            foreach (var offset in offsets)
                GetPostingsFromStream(keyId, offset, documents);

            if (_logger != null)
                _logger.LogTrace($"read {documents.Count} postings into memory in {time.Elapsed}");

            return documents;
        }

        private void GetPostingsFromStream(long keyId, long postingsOffset, HashSet<(ulong collectionId, long docId)> postings)
        {
            // seek to header
            _stream.Seek(postingsOffset, SeekOrigin.Begin);

            var headerLen = sizeof(long) * 2;

            // read header
            var headerBuf = ArrayPool<byte>.Shared.Rent(headerLen);
            _stream.Read(headerBuf, 0, headerLen);
            var listId = BitConverter.ToInt64(headerBuf);
            var addressOfNextPage = BitConverter.ToInt64(headerBuf, sizeof(long));
            ArrayPool<byte>.Shared.Return(headerBuf);

            // read postings
            var postingsList = _documentSession.ReadDocumentValue<IList<long>>(
                listId, 
                keyId, 
                _documentSession.GetOrCreateDocumentReader(Database.GetIndexCollectionId(_collectionId)));

            foreach (var docId in postingsList)
            {
                postings.Add((_collectionId, docId));
            }

            if (addressOfNextPage > 0)
            {
                GetPostingsFromStream(keyId, addressOfNextPage, postings);
            }
        }

        public void Dispose()
        {
            _stream.Dispose();
            _documentSession.Dispose();
        }
    }
}
