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
        private readonly Stream _postingsStream;
        private readonly PostingsIndexReader _postingsIndex;
        private readonly ILogger _logger;
        private readonly ulong _collectionId;

        public PostingsReader(string directory, ulong collectionId, long keyId, ISessionFactory streamDispatcher, ILogger logger = null)
            : this(
                  streamDispatcher.CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pos")), 
                  new PostingsIndexReader(streamDispatcher.CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pix"))), 
                  collectionId, 
                  logger
                  ) 
        { }

        public PostingsReader(Stream postingsStream, PostingsIndexReader postingsIndex, ulong collectionId, ILogger logger = null)
        {
            _postingsStream = postingsStream;
            _postingsIndex = postingsIndex;
            _logger = logger;
            _collectionId = collectionId;
        }

        public IList<(ulong, long)> Read(long keyId, IList<long> pageIds)
        {
            var time = Stopwatch.StartNew();
            var documents = new List<(ulong, long)>();

            foreach (var pageId in pageIds)
                GetPostingsFromStream(keyId, pageId, documents);

            if (_logger != null)
                _logger.LogTrace($"read {documents.Count} postings into memory in {time.Elapsed}");

            return documents;
        }

        private void GetPostingsFromStream(long keyId, long postingsPageId, List<(ulong collectionId, long docId)> documents)
        {
            var pageInfo = _postingsIndex.GetPageInfo(postingsPageId);

            _postingsStream.Seek(pageInfo.address, SeekOrigin.Begin);

            const int headerLen = sizeof(long);
            var headerBuf = ArrayPool<byte>.Shared.Rent(headerLen);

            _postingsStream.Read(headerBuf, 0, headerLen);

            var numOfPostings = BitConverter.ToInt64(headerBuf);

            ArrayPool<byte>.Shared.Return(headerBuf);

            var listLen = sizeof(long) * numOfPostings;
            var listBuf = new byte[listLen];
            var read = _postingsStream.Read(listBuf);

            if (read != listLen)
                throw new Exception($"registered lenght was {listLen} but we acctually read {read}");

            foreach (var docId in MemoryMarshal.Cast<byte, long>(listBuf))
            {
                documents.Add((_collectionId, docId));
            }          

            if (pageInfo.nextPageId > 0)
            {
                GetPostingsFromStream(keyId, pageInfo.nextPageId, documents);
            }
        }

        public void Dispose()
        {
            _postingsStream.Dispose();
        }
    }
}
