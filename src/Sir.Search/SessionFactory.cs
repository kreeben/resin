using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Documents;
using Sir.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    /// <summary>
    /// Stream dispatcher with helper methods for writing, indexing, optimizing, updating and truncating document collections.
    /// </summary>
    public class SessionFactory : IDisposable, ISessionFactory
    {
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<(string directory, ulong collectionId), IDictionary<long, IList<VectorNode>>> _columns;
        public SessionFactory(ILogger logger = null)
        {
            _logger = logger;
            _columns = new ConcurrentDictionary<(string directory, ulong collectionId), IDictionary<long, IList<VectorNode>>>();

            LogTrace($"database initiated");
        }

        public IndexWriteStream CreateIndexingWriteStream(string directory, ulong collectionId, long keyId, bool keepOpen = false)
        {
            return new IndexWriteStream(
                indexStream: CreateAppendStream(directory, collectionId, keyId, "ix"),
                vectorStream: CreateAppendStream(directory, collectionId, keyId, "vec"),
                postingsStream: CreateAppendStream(directory, collectionId, keyId, "pos"),
                pageIndexStream: CreateAppendStream(directory, collectionId, keyId, "ixtp"),
                postingsIndexReadStream: CreateReadStream(Path.Combine(directory, $"{collectionId}.{keyId}.pix")),
                postingsIndexUpdateStream: CreateSeekWriteStream(directory, collectionId, keyId, "pix"),
                postingsIndexAppendStream: CreateAppendStream(directory, collectionId, keyId, "pix"),
                keepOpen: keepOpen
                );
        }

        public IDictionary<long, IList<VectorNode>> GetColumns(string directory, ulong collectionId, IModel model)
        {
            var columns = new Dictionary<long, IList<VectorNode>>();

            if (string.IsNullOrWhiteSpace(directory))
            {
                return columns;
            }

            if (_columns.TryGetValue((directory, collectionId), out var col))
            {
                return col;
            }

            foreach (var ixFileName in Directory.GetFiles(directory, "*.ix", SearchOption.AllDirectories))
            {
                var name = Path.GetFileNameWithoutExtension(ixFileName);
                var i = name.LastIndexOf('.') + 1;
                var keyIdSegment = name.Substring(i, name.Length - i);
                var keyId = long.Parse(keyIdSegment);
                var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");
                var pageIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.ixtp");

                IList<(long offset, long length)> pages;

                using (var pageIndexReader = new PageIndexReader(CreateReadStream(pageIndexFileName)))
                {
                    pages = pageIndexReader.ReadAll();
                }

                using (var ixStream = CreateReadStream(ixFileName))
                using (var vectorStream = CreateReadStream(vectorFileName))
                {
                    var trees = new List<VectorNode>();

                    foreach (var page in pages)
                    {
                        ixStream.Seek(page.offset, SeekOrigin.Begin);
                        var tree = PathFinder.DeserializeTree(ixStream, vectorStream, model, page.length);
                        trees.Add(tree);
                    }

                    columns.Add(keyId, trees);
                }

                LogInformation($"loaded {ixFileName} into memory");
            }

            if (columns.Count > 0)
            {
                _columns.GetOrAdd((directory, collectionId), columns);

                return columns;
            }

            return new Dictionary<long, IList<VectorNode>>();
        }

        public void UpdateColumns(string directory, ulong collectionId, IDictionary<long, IList<VectorNode>> columns)
        {
            _columns[(directory, collectionId)] = columns;
        }

        public ColumnReader CreateColumnReader(string directory, ulong collectionId, long keyId, IModel model)
        {
            var ixFileName = Path.Combine(directory, string.Format("{0}.{1}.ix", collectionId, keyId));
            var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");
            var pageIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.ixtp");

            if (File.Exists(ixFileName) && File.Exists(vectorFileName) && File.Exists(pageIndexFileName))
            {
                using (var pageIndexReader = new PageIndexReader(CreateReadStream(pageIndexFileName)))
                {
                    return new ColumnReader(
                        pageIndexReader.ReadAll(),
                        CreateReadStream(ixFileName),
                        CreateReadStream(vectorFileName),
                        model,
                        _logger);
                }
            }

            return null;
        }

        public IEnumerable<Document> Select(string directory, ulong collectionId, HashSet<string> select, int skip = 0, int take = 0)
        {
            using (var reader = new DocumentStreamSession(this, new KeyRepository(directory, this), directory))
            {
                foreach (var document in reader.ReadDocuments(collectionId, select, skip, take))
                {
                    yield return document;
                }
            }
        }

        private void LogInformation(string message)
        {
            if (_logger != null)
                _logger.LogInformation(message);
        }

        private void LogTrace(string message)
        {
            if (_logger != null)
                _logger.LogTrace(message);
        }

        private void LogDebug(string message)
        {
            if (_logger != null)
                _logger.LogDebug(message);
        }

        private void LogError(Exception ex, string message)
        {
            if (_logger != null)
                _logger.LogError(ex, message);
        }

        public long GetDocCount(string directory, string collection)
        {
            var fileName = Path.Combine(directory, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / DocIndexWriter.BlockSize;
        }

        public void Truncate(string directory, ulong collectionId)
        {
            var count = 0;

            if (Directory.Exists(directory))
            {
                foreach (var file in Directory.GetFiles(directory, $"{collectionId}*"))
                {
                    File.Delete(file);
                    count++;
                }

                var keyStr = Path.Combine(directory, collectionId.ToString());
                var key = keyStr.ToHash();
                new KeyRepository(directory, this).RemoveFromCache(key);
            }

            LogInformation($"truncated collection {collectionId} ({count} files affected)");
        }

        public void TruncateIndex(string directory, ulong collectionId)
        {
            var count = 0;

            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.pix"))
            {
                File.Delete(file);
                count++;
            }

            LogInformation($"truncated index {collectionId} ({count} files affected)");
        }

        public void Rename(string directory, ulong currentCollectionId, ulong newCollectionId)
        {
            var count = 0;

            var from = currentCollectionId.ToString();
            var to = newCollectionId.ToString();

            foreach (var file in Directory.GetFiles(directory, $"{currentCollectionId}*"))
            {
                File.Move(file, file.Replace(from, to));
                count++;
            }

            var key = Path.Combine(directory, currentCollectionId.ToString()).ToHash();

            new KeyRepository(directory, this).RemoveFromCache(key);

            LogInformation($"renamed collection {currentCollectionId} to {newCollectionId} ({count} files affected)");
        }

        public void Optimize<T>(
            string directory,
            string collection,
            HashSet<string> selectFields, 
            IModel<T> model,
            IIndexReadWriteStrategy indexStrategy,
            int skipDocuments = 0,
            int takeDocuments = 0,
            int reportFrequency = 1000,
            int pageSize = 100000)
        {
            var collectionId = collection.ToHash();

            LogDebug($"optimizing indices for {string.Join(',', selectFields)} in collection {collectionId}");

            using (var debugger = new IndexDebugger(_logger, reportFrequency))
            using (var documents = new DocumentStreamSession(this, new KeyRepository(directory, this), directory))
            {
                using (var writeQueue = new ProducerConsumerQueue<IndexSession<T>>(indexSession =>
                {
                    indexSession.Commit();
                }))
                {
                    var took = 0;
                    var skip = skipDocuments;

                    while (took < takeDocuments)
                    {
                        var payload = documents.GetDocumentsAsVectors(
                            collectionId,
                            selectFields,
                            model,
                            false,
                            skip,
                            pageSize);

                        var count = 0;

                        using (var indexSession = new IndexSession<T>(model, indexStrategy, this, directory, collectionId))
                        {
                            foreach (var document in payload)
                            {
                                foreach (var node in document.Nodes)
                                {
                                    foreach (var n in PathFinder.All(node))
                                    {
                                        indexSession.Put(document.DocumentId, n.KeyId.Value, new ISerializableVector[] { n.Vector });
                                    }
                                }

                                count++;

                                debugger.Step(indexSession);
                            }

                            writeQueue.Enqueue(indexSession);
                        }

                        if (count == 0)
                            break;

                        took += count;
                        skip += pageSize;
                    }
                }
            }

            LogDebug($"optimized collection {collection}");
        }

        public void StoreDataAndBuildInMemoryIndex<T>(IEnumerable<Document> job, WriteSession writeSession, IndexSession<T> indexSession, int reportSize = 1000, bool label = true)
        {
            var debugger = new IndexDebugger(_logger, reportSize);

            foreach (var document in job)
            {
                writeSession.Put(document);

                foreach (var field in document.Fields)
                {
                    if (field.Value != null)
                    {
                        indexSession.Put(document.Id, field.KeyId, (T)field.Value, label);
                    }
                }

                debugger.Step(indexSession);
            }
        }

        public void StoreDataAndBuildInMemoryIndex<T>(
            Document document, 
            WriteSession writeSession, 
            IndexSession<T> indexSession, 
            bool label = true)
        {
            writeSession.Put(document);

            foreach (var field in document.Fields)
            {
                if (field.Value != null && field.Value is T typedValue)
                {
                    indexSession.Put(document.Id, field.KeyId, typedValue, label);
                }
            }
        }

        public void StoreDataAndPersistIndex<T>(string directory, ulong collectionId, IEnumerable<Document> job, IModel<T> model, IIndexReadWriteStrategy indexStrategy, int reportSize = 1000)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(this, directory, collectionId)))
            using (var indexSession = new IndexSession<T>(model, indexStrategy, this, directory, collectionId))
            {
                StoreDataAndBuildInMemoryIndex(job, writeSession, indexSession, reportSize);

                indexSession.Commit();
            }
        }

        public void Store(string directory, ulong collectionId, IEnumerable<Document> job)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(this, directory, collectionId)))
            {
                foreach (var document in job)
                    writeSession.Put(document);
            }
        }

        public void Index<T>(string directory, ulong collectionId, IModel<T> model, IIndexReadWriteStrategy indexStrategy, HashSet<string> fieldsOfInterest, int reportSize = 1000, int pageSize = 1000, int skip = 0, int take = int.MaxValue)
        {
            using (var debugger = new IndexDebugger(_logger, reportSize))
            using (var documents = new DocumentStreamSession(this, new KeyRepository(directory, this), directory))
            using (var indexSession = new IndexSession<T>(model, indexStrategy, this, directory, collectionId, _logger))
            {
                foreach (var batch in documents.ReadDocuments(collectionId, fieldsOfInterest, skip, take).Batch(pageSize))
                {
                    foreach (var document in batch)
                    {
                        foreach (var field in document.Fields)
                        {
                            indexSession.Put(document.Id, field.KeyId, (T)field.Value, label: false);
                        }

                        debugger.Step(indexSession);
                    }
                    indexSession.Commit();
                }
            }
        }

        public bool DocumentExists<T>(string directory, string collection, string key, T value, IModel<T> model, bool label = true)
        {
            var keyRepository = new KeyRepository(directory, this);
            var query = new QueryParser<T>(directory, keyRepository, model, logger: _logger)
                .Parse(collection, value, key, key, and: true, or: false, label);

            if (query != null)
            {
                using (var searchSession = new SearchSession(directory, keyRepository, this, model, new LogStructuredIndexingStrategy(model),  _logger))
                {
                    var document = searchSession.SearchScalar(query);

                    if (document != null)
                    {
                        if (document.Score >= model.IdenticalAngle)
                            return true;
                    }
                }
            }

            return false;
        }

        public FileStream CreateLockFile(string directory, ulong collectionId)
        {
            return new FileStream(Path.Combine(directory, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        public Stream CreateAsyncReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        }

        public Stream CreateReadStream(string fileName, int bufferSize = 4096)
        {
            LogTrace($"opening {fileName}");

            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, FileOptions.SequentialScan);
        }

        public Stream CreateAsyncAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        }

        public Stream CreateAppendStream(string directory, ulong collectionId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {}
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateAppendStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                LogTrace($"creating {fileName}");

                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {}
            }

            LogTrace($"opening {fileName}");

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateSeekWriteStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                LogTrace($"creating {fileName}");

                using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) { }
            }

            LogTrace($"opening {fileName}");

            return new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        public void Dispose()
        {
            LogTrace($"database disposed");
        }
    }
}