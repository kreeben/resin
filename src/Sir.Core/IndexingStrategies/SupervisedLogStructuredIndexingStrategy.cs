using Microsoft.Extensions.Logging;
using Sir.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

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

        public void Commit(string directory, ulong collectionId, long keyId, VectorNode tree, IStreamDispatcher streamDispatcher, Dictionary<(long keyId, long pageId), HashSet<long>> postingsToAppend, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();

            using (var vectorStream = streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "vec"))
            using (var postingsWriter = new PostingsWriter(
                    streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "pos"),
                    new PostingsIndexAppender(streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "pix")),
                    new PostingsIndexUpdater(streamDispatcher.CreateSeekWriteStream(directory, collectionId, keyId, "pix")),
                    new PostingsIndexReader(streamDispatcher.CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pix")))
            ))
            using (var columnWriter = new ColumnWriter(streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "ix")))
            using (var pageIndexWriter = new PageIndexWriter(streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "ixtp")))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsWriter, pageIndexWriter, postingsToAppend);

                if (logger != null)
                    logger.LogDebug($"serialized column {keyId}, weight {tree.Weight} {size} in {time.Elapsed}");
            }
        }
    }
}