using System.Collections.Generic;

namespace Sir.Strings
{
    public interface IReducer
    {
        void Reduce(Query mappedQuery, ref IDictionary<(ulong, long), double> result);
    }
}