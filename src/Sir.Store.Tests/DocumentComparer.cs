using System.Collections.Generic;

namespace Sir.Search.Tests
{
    public class DocumentComparer : IEqualityComparer<IDictionary<string, object>>
    {
        public bool Equals(IDictionary<string, object> x, IDictionary<string, object> y)
        {
            return (long)x[SystemFields.DocumentId] == (long)y[SystemFields.DocumentId];
        }

        public int GetHashCode(IDictionary<string, object> obj)
        {
            return obj.GetHashCode();
        }
    }
}