using Microsoft.Extensions.Logging;
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
        private readonly IStreamDispatcher _streamDispatcher;
        private readonly Stream _stream;
        private readonly ILogger _logger;

        public PostingsReader(string directory, ulong collectionId, long keyId, IStreamDispatcher streamDispatcher, ILogger logger = null)
        {
            _streamDispatcher = streamDispatcher;
            _stream = GetOrCreateStream(directory, collectionId, keyId);
            _logger = logger;
        }

        public IList<(ulong, long)> Read(ulong collectionId, long keyId, long offset)
        {
            var time = Stopwatch.StartNew();
            var documents = new List<(ulong, long)>();

            GetPostingsFromStream(collectionId, keyId, offset, documents);

            if (_logger!=null)
                _logger.LogTrace($"read {documents.Count} postings into memory in {time.Elapsed}");

            return documents;
        }

        public IList<(ulong, long)> Read(ulong collectionId, long keyId, IList<long> offsets)
        {
            var time = Stopwatch.StartNew();
            var documents = new List<(ulong, long)>();

            foreach (var offset in offsets)
                GetPostingsFromStream(collectionId, keyId, offset, documents);

            if (_logger != null)
                _logger.LogTrace($"read {documents.Count} postings into memory in {time.Elapsed}");

            return documents;
        }

        private void GetPostingsFromStream(ulong collectionId, long keyId, long postingsOffset, IList<(ulong collectionId, long docId)> documents)
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
                documents.Add((collectionId, docId));
            }

            if (addressOfNextPage > 0)
            {
                GetPostingsFromStream(collectionId, keyId, addressOfNextPage, documents);
            }
        }

        private Stream GetOrCreateStream(string directory, ulong collectionId, long keyId)
        {
            return _streamDispatcher.CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pos"));
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
