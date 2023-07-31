using Microsoft.Extensions.Logging;
using Sir.IO;
using System.Collections.Generic;

namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void Put<T>(VectorNode column, VectorNode node);
        Hit GetMatchOrNull(ISerializableVector vector, ColumnReader reader);
        void Commit(string directory, ulong collectionId, long keyId, VectorNode tree, ISessionFactory streamDispatcher, Dictionary<(long keyId, long pageId), HashSet<long>> postingsToAppend, ILogger logger = null);
    }
}
