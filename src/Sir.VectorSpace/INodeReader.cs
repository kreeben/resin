using System;

namespace Sir.VectorSpace
{
    public interface INodeReader : IDisposable
    {
        long KeyId { get; }
        Hit ClosestTerm(IVector vector, IStringModel model, long keyId);
    }
}
