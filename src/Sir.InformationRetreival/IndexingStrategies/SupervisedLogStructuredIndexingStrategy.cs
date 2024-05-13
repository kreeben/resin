using Microsoft.Extensions.Logging;
using Sir.IO;
using Sir.KeyValue;
using System.Diagnostics;

namespace Sir
{
    public class SupervisedLogStructuredIndexingStrategy : IIndexReadWriteStrategy
    {
        private readonly IModel _model;

        public SupervisedLogStructuredIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public Hit GetMatchOrNull(ISerializableVector vector, IModel model, ColumnReader reader)
        {
            return reader.ClosestMatchOrNullScanningAllPages(vector, model);
        }

        public void Put<T>(VectorNode column, VectorNode node)
        {
            column.AddOrAppendSupervised(node, _model);
        }

        public void SerializePage(string directory, ulong collectionId, long keyId, VectorNode tree, IndexCache indexCache, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();

            using (var vectorStream = StreamFactory.CreateAppendStream(directory, collectionId, keyId, "vec"))
            using (var postingsWriter = new PostingsWriter(StreamFactory.CreateSeekableWriteStream(directory, collectionId, keyId, "pos"), indexCache))
            using (var columnWriter = new ColumnWriter(StreamFactory.CreateAppendStream(directory, collectionId, keyId, "ix")))
            using (var pageIndexWriter = new PageIndexWriter(StreamFactory.CreateAppendStream(directory, collectionId, keyId, "ixtp")))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsWriter, pageIndexWriter);

                if (logger != null)
                    logger.LogInformation($"serialized column {keyId}, weight {tree.Weight} {size} in {time.Elapsed}");
            }
        }
    }
}