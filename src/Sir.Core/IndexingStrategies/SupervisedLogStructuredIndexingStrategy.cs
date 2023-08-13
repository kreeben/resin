using Sir.IO;

namespace Sir
{
    public class SupervisedLogStructuredIndexingStrategy : IIndexReadWriteStrategy
    {
        private readonly IModel _model;

        public SupervisedLogStructuredIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public Hit GetMatchOrNull(ISerializableVector vector, ColumnReader reader)
        {
            return reader.ClosestMatchOrNullScanningAllPages(vector);
        }

        public void Put<T>(VectorNode column, VectorNode node)
        {
            column.AddOrAppendSupervised(node, _model);
        }
    }
}