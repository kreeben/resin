using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.Search
{
    /// <summary>
    /// Read (reduce) postings.
    /// </summary>
    public class PostingsReader : Reducer, IPostingsReader
    {
        private readonly ISessionFactory _sessionFactory;
        private readonly ConcurrentDictionary<ulong, Stream> _streams;

        public PostingsReader(ISessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
            _streams = new ConcurrentDictionary<ulong, Stream>();
        }

        protected override IList<(ulong, long)> Read(ulong collectionId, IList<long> offsets)
        {
            var collectionRef = _sessionFactory.GetCollectionReference(collectionId);

            var list = new List<(ulong, long)>();

            foreach (var postingsOffset in offsets)
                GetPostingsFromStream(collectionRef, postingsOffset, list);

            return list;
        }

        //public IDictionary<(ulong, long), double> ReadWithPredefinedScore(ulong collectionId, IList<long> offsets, double score)
        //{
        //    var collectionRef = _sessionFactory.GetCollectionReference(collectionId);

        //    var result = new Dictionary<(ulong, long), double>();

        //    foreach (var offset in offsets)
        //    {
        //        GetPostingsFromStream(collectionRef, offset, result, score);
        //    }

        //    return result;
        //}

        private void GetPostingsFromStream(ulong collectionId, long postingsOffset, IDictionary<(ulong collectionId, long docId), double> result, double score)
        {
            var list = new List<(ulong, long)>();

            GetPostingsFromStream(collectionId, postingsOffset, list);

            foreach (var id in list)
            {
                result.Add(id, score);
            }
        }

        private void GetPostingsFromStream(ulong collectionId, long postingsOffset, IList<(ulong collectionId, long docId)> result)
        {
            var stream = GetOrCreateStream(collectionId);

            stream.Seek(postingsOffset, SeekOrigin.Begin);

            Span<byte> buf = new byte[sizeof(long)];

            stream.Read(buf);

            var numOfPostings = BitConverter.ToInt64(buf);

            var len = sizeof(long) * (int)numOfPostings;

            Span<byte> listBuf = new byte[len];

            var read = stream.Read(listBuf);

            if (read != len)
                throw new DataMisalignedException();

            foreach (var docId in MemoryMarshal.Cast<byte, long>(listBuf))
            {
                result.Add((collectionId, docId));
            }
        }

        private Stream GetOrCreateStream(ulong collectionId)
        {
            return _streams.GetOrAdd(
                collectionId,
                _sessionFactory.CreateReadStream(
                    Path.Combine(_sessionFactory.Dir, $"{collectionId}.pos"))
                );
        }

        public void Dispose()
        {
            foreach (var stream in _streams.Values)
                stream.Dispose();
        }
    }
}
