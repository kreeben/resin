using Microsoft.Extensions.Logging;
using Sir.IO;

namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void Put<T>(VectorNode column, VectorNode node);
        Hit GetMatchOrNull(ISerializableVector vector, IModel model, ColumnReader reader);
        void SerializePage(string directory, ulong collectionId, long keyId, VectorNode tree, VectorNode postings, IndexIndex indexCache, ILogger logger = null);
    }
}
