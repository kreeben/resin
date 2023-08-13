using Sir.IO;

namespace Sir
{
    public class LogStructuredIndexingStrategy : IIndexReadWriteStrategy
    {
        private readonly IModel _model;

        public LogStructuredIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public Hit GetMatchOrNull(ISerializableVector vector, ColumnReader reader)
        {
            return reader.ClosestMatchOrNullStoppingAtFirstIdenticalPage(vector);
        }

        public void Put<T>(VectorNode column, VectorNode node)
        {
            column.AddOrAppend(node, _model);
        }
    }
}