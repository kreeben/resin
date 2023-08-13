using Sir.IO;

namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void Put<T>(VectorNode column, VectorNode node);
        Hit GetMatchOrNull(ISerializableVector vector, ColumnReader reader);
    }
}
