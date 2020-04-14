using System;

namespace Sir.Search
{
    public interface IReadSession : IDisposable
    {
        ReadResult Read(IQuery query, int skip, int take);
    }
}