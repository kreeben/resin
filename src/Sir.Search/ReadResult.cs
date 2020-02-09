using System.Collections.Generic;

namespace Sir.Search
{
    public class ReadResult
    {
        public IQuery Query { get; set; }
        public long Total { get; set; }
        public IEnumerable<IDictionary<string, object>> Docs { get; set; }
    }
}
