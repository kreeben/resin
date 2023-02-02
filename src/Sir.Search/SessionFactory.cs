using MathNet.Numerics.LinearAlgebra;
using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Documents;
using Sir.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir
{
    /// <summary>
    /// Stream dispatcher with helper methods for writing, indexing, optimizing, updating and truncating document collections.
    /// </summary>
    public class SessionFactory : IDisposable, IStreamDispatcher
    {
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;
        private ILogger _logger;

        public SessionFactory(ILogger logger = null)
        {
            _logger = logger;
            _keys = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();

            LogTrace($"database initiated");
        }

        public IDictionary<long, Vector<float>> DeserializeMeanVectors(IModel model, string directory, ulong collectionId)
        {
            var result = new Dictionary<long, Vector<float>>();

            foreach (var keyId in AllKeyIds(directory, collectionId))
            {
                //var vectorIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vecix");
                //var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");

                //using (var vectorIndexStream = CreateReadStream(vectorIndexFileName))
                //using (var vectorStream = CreateReadStream(vectorFileName))
                //{
                //    Span<byte> obuf = new byte[sizeof(long)];
                //    vectorIndexStream.Read(obuf);
                //    var vectorOffset = BitConverter.ToInt64(obuf);

                //    Span<byte> cbuf = new byte[sizeof(int)];
                //    vectorIndexStream.Read(cbuf);
                //    var componentCount = BitConverter.ToInt32(cbuf);

                //    var vector = DeserializeVector(model.NumOfDimensions, vectorOffset, componentCount, vectorStream);

                //    result.Add(keyId, vector);
                //}

                var vector = CreateVector.Sparse<float>(model.NumOfDimensions, 1);
                result.Add(keyId, vector);
            }

            return result;
        }

        private static Vector<float> DeserializeVector(int numOfDimensions, long vectorOffset, int componentCount, Stream vectorStream)
        {
            Span<byte> buf = new byte[componentCount * 2 * sizeof(float)];

            vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, float>(buf.Slice(componentCount * sizeof(float)));
            var tuples = new Tuple<int, float>[componentCount];

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            return CreateVector.SparseOfIndexed(numOfDimensions, tuples);
        }

        public ColumnReader CreateColumnReader(string directory, ulong collectionId, long keyId, IModel model)
        {
            var ixFileName = Path.Combine(directory, string.Format("{0}.{1}.ix", collectionId, keyId));
            var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");
            var pageIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.ixtp");

            using (var pageIndexReader = new PageIndexReader(CreateReadStream(pageIndexFileName)))
            {
                return new ColumnReader(
                    pageIndexReader.ReadAll(),
                    CreateReadStream(ixFileName),
                    CreateReadStream(vectorFileName),
                    DeserializeMeanVectors(model, directory, collectionId)[keyId]);
            }
        }

        public IEnumerable<Document> Select(string directory, ulong collectionId, HashSet<string> select, int skip = 0, int take = 0)
        {
            using (var reader = new DocumentStreamSession(directory, this))
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
                _keys.Remove(key, out _);
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

            _keys.Remove(key, out _);

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
            using (var documents = new DocumentStreamSession(directory, this))
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
                                    indexSession.Put(node);
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

        public bool DocumentExists<T>(string directory, string collection, string key, T value, IModel<T> model, bool label = true)
        {
            var query = new QueryParser<T>(directory, this, model, logger: _logger)
                .Parse(collection, value, key, key, and: true, or: false, label);

            if (query != null)
            {
                using (var searchSession = new SearchSession(directory, this, model, new LogStructuredIndexingStrategy(model),  _logger))
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

        private void ReadKeysIntoCache(string directory)
        {
            foreach (var keyFile in Directory.GetFiles(directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                var key = Path.Combine(directory, collectionId.ToString()).ToHash();

                var keys = _keys.GetOrAdd(key, (k) =>
                {
                    var ks = new ConcurrentDictionary<ulong, long>();

                    using (var stream = new FileStream(keyFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                    {
                        long i = 0;
                        var buf = new byte[sizeof(ulong)];
                        var read = stream.Read(buf, 0, buf.Length);

                        while (read > 0)
                        {
                            ks.TryAdd(BitConverter.ToUInt64(buf, 0), i++);

                            read = stream.Read(buf, 0, buf.Length);
                        }
                    }

                    return ks;
                });
            }
        }

        public void RegisterKeyMapping(string directory, ulong collectionId, ulong keyHash, long keyId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();
            var keys = _keys.GetOrAdd(key, (key) => { return new ConcurrentDictionary<ulong, long>(); });
            var keyMapping = keys.GetOrAdd(keyHash, (key) =>
            {
                using (var stream = CreateAppendStream(directory, collectionId, "kmap"))
                {
                    stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                }
                return keyId;
            });
        }

        public IEnumerable<long> AllKeyIds(string directory, ulong collectionId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            if (!_keys.TryGetValue(key, out _))
            {
                ReadKeysIntoCache(directory);
            }

            ConcurrentDictionary<ulong, long> keys;

            if (_keys.TryGetValue(key, out keys))
            {
                foreach (var keyId in keys.Values)
                    yield return keyId;
            }
        }

        public long GetKeyId(string directory, ulong collectionId, ulong keyHash)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(key, out keys))
            {
                ReadKeysIntoCache(directory);
            }

            if (keys != null || _keys.TryGetValue(key, out keys))
            {
                return keys[keyHash];
            }

            throw new Exception($"unable to find key {keyHash} for collection {collectionId} in directory {directory}.");
        }

        public bool TryGetKeyId(string directory, ulong collectionId, ulong keyHash, out long keyId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(key, out keys))
            {
                ReadKeysIntoCache(directory);
            }

            if (keys != null || _keys.TryGetValue(key, out keys))
            {
                if (keys.TryGetValue(keyHash, out keyId))
                {
                    return true;
                }
            }

            keyId = -1;
            return false;
        }

        public Stream CreateAsyncReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        }

        public Stream CreateReadStream(string fileName)
        {
            LogTrace($"opening {fileName}");

            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
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

        public void Dispose()
        {
            LogTrace($"database disposed");
        }
    }
}