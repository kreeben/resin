using Sir.Documents;
using System.Collections.Generic;

namespace Sir
{
    public class SearchResult
    {
        public Query Query { get; }
        public long Total { get; }
        public IEnumerable<Document> Documents { get; }
        public int Count { get; }

        public SearchResult(Query query, long total, int count, IEnumerable<Document> documents)
        {
            Query = query;
            Total = total;
            Count = count;
            Documents = documents;
        }
    }
}
