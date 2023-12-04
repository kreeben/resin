using Microsoft.Extensions.Logging;
using Sir.IO;

namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void Put<T>(VectorNode column, VectorNode node);
        Hit GetMatchOrNull(ISerializableVector vector, IModel model, ColumnReader reader);
        void Commit(string directory, ulong collectionId, long keyId, VectorNode tree, ILogger logger = null);
    }
}
