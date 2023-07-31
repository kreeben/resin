using Microsoft.Extensions.Logging;
using Sir.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

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

        public void Commit(string directory, ulong collectionId, long keyId, VectorNode tree, ISessionFactory sessionFactory, Dictionary<(long keyId, long pageId), HashSet<long>> postingsToAppend, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();

            using (var vectorStream = sessionFactory.CreateAppendStream(directory, collectionId, keyId, "vec"))
            using(var postingsWriter = new PostingsWriter(
                    sessionFactory.CreateAppendStream(directory, collectionId, keyId, "pos"),
                    new PostingsIndexAppender(sessionFactory.CreateAppendStream(directory, collectionId, keyId, "pix")),
                    new PostingsIndexUpdater(sessionFactory.CreateSeekWriteStream(directory, collectionId, keyId, "pix")),
                    new PostingsIndexReader(sessionFactory.CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pix")))))
            using (var columnWriter = new ColumnWriter(sessionFactory.CreateAppendStream(directory, collectionId, keyId, "ix")))
            using (var pageIndexWriter = new PageIndexWriter(sessionFactory.CreateAppendStream(directory, collectionId, keyId, "ixtp")))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsWriter, pageIndexWriter, postingsToAppend);

                if (logger != null)
                    logger.LogDebug($"serialized column {keyId}, weight {tree.Weight} {size} in {time.Elapsed}");
            }
        }
    }
}