using Microsoft.Extensions.Logging;
using Sir.Documents;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

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

        public PostingsReader(string directory, ulong collectionId, long keyId, ILogger logger = null)
        {
            _stream = DocumentInfoReader.CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pos"));
            _logger = logger;
            _collectionId = collectionId;
        }

        public IList<(ulong, long)> Read(long keyId, IList<long> offsets)
        {
            var time = Stopwatch.StartNew();
            var documents = new List<(ulong, long)>();

            foreach (var offset in offsets)
                GetPostingsFromStream(keyId, offset, documents);

            if (_logger != null)
                _logger.LogTrace($"read {documents.Count} postings into memory in {time.Elapsed}");

            return documents;
        }

        private void GetPostingsFromStream(long keyId, long postingsOffset, List<(ulong collectionId, long docId)> documents)
        {
            _stream.Seek(postingsOffset, SeekOrigin.Begin);

            var headerLen = sizeof(long) * 2;
            var headerBuf = ArrayPool<byte>.Shared.Rent(headerLen);

            _stream.Read(headerBuf, 0, headerLen);

            var numOfPostings = BitConverter.ToInt64(headerBuf);
            var addressOfNextPage = BitConverter.ToInt64(headerBuf, sizeof(long));

            ArrayPool<byte>.Shared.Return(headerBuf);

            var listLen = sizeof(long) * numOfPostings;
            var listBuf = new byte[listLen];
            var read = _stream.Read(listBuf);

            if (read != listLen)
                throw new Exception($"list lenght was {listLen} but read length was {read}");

            foreach (var docId in MemoryMarshal.Cast<byte, long>(listBuf))
            {
                documents.Add((_collectionId, docId));
            }

            if (addressOfNextPage > 0)
            {
                GetPostingsFromStream(keyId, addressOfNextPage, documents);
            }
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
