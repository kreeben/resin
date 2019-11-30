using System;
using System.Collections.Generic;

namespace Sir
{
    public interface IPostingsReader : IDisposable
    {
        void Reduce(Query mappedQuery, IDictionary<(ulong, long), double> result);
        IDictionary<(ulong, long), double> ReadWithPredefinedScore(ulong collectionId, IList<long> offsets, double score);
    }
}