using System.Collections.Generic;

namespace Sir.Strings
{
    public interface IReducer
    {
        void Reduce(IQuery mappedQuery, ref IDictionary<(ulong, long), double> result);
    }
}