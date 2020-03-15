using System;

namespace Sir.VectorSpace
{
    public interface IColumnReader : IDisposable
    {
        Hit ClosestMatch(IVector vector, IStringModel model);
    }
}
