using System;
using System.Collections.Generic;

namespace Sir.Search
{
    public interface IReadSession : IDisposable
    {
        ReadResult Read(IQuery query, int skip, int take);
        IList<IDictionary<string, object>> ReadDocs(
            IEnumerable<KeyValuePair<(
                ulong collectionId, long docId), double>> docs, 
                int scoreDivider,
                HashSet<string> select
            );
    }
}