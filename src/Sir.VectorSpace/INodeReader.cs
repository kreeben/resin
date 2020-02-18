using System;

namespace Sir.VectorSpace
{
    public interface INodeReader : IDisposable
    {
        Hit ClosestTerm(IVector vector, IStringModel model, long keyId);
    }
}
