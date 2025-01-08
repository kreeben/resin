using Microsoft.Extensions.Logging;
using Sir.Documents;
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

        public void SerializePage(string directory, ulong collectionId, long keyId, VectorNode tree, VectorNode postings, IndexIndex indexCache, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();
            var indexCollectionId = Database.GetIndexCollectionId(collectionId);

            using (var writeSession = new WriteSession(new DocumentRegistryWriter(directory, indexCollectionId)))
            using (var vectorStream = StreamFactory.CreateAppendStream(directory, indexCollectionId, keyId, "vec"))
            using (var postingsWriter = new PostingsWriter(StreamFactory.CreateSeekableWriteStream(directory, indexCollectionId, keyId, "pos"), writeSession, indexCache))
            using (var columnWriter = new ColumnWriter(StreamFactory.CreateAppendStream(directory, indexCollectionId, keyId, "ix")))
            using (var pageIndexWriter = new PageIndexWriter(StreamFactory.CreateAppendStream(directory, indexCollectionId, keyId, "ixtp")))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsWriter, pageIndexWriter);

                if (logger != null)
                    logger.LogInformation($"serialized column {keyId}, weight {tree.Weight} {size} in {time.Elapsed}");

                foreach (var node in postings.All())
                {
                    postingsWriter.SerializePostings(node.Documents, node.KeyId.Value, node.Vector);
                }
            }
        }
    }
}