using System.Collections.Generic;

namespace Sir
{
    public class ScoredResult
    {
        public IList<KeyValuePair<(ulong, long), double>> SortedDocuments { get; set; }
        public int Total { get; set; }
    }
}
