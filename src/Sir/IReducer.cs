using System;
using System.Collections.Generic;

namespace Sir
{
    public interface IReducer
    {
        void Reduce(Query mappedQuery, ref IDictionary<(ulong, long), double> result);
    }
}