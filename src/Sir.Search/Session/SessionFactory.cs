﻿using Microsoft.Extensions.Logging;
using Sir.Document;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sir.Search
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ISessionFactory
    {
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;

        public string Directory { get; }
        public ILogger Logger { get; }

        public SessionFactory(string directory = null, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();

            Directory = directory;

            if (Directory != null && !System.IO.Directory.Exists(Directory))
            {
                System.IO.Directory.CreateDirectory(Directory);
            }

            _keys = LoadKeys();
            Logger = logger;

           Log($"sessionfactory initiated in {time.Elapsed}");
        }

        private void Log(string message)
        {
            if (Logger != null)
                Logger.LogInformation(message);
        }

        public long GetDocCount(string collection)
        {
            var fileName = Path.Combine(Directory, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / (sizeof(long) + sizeof(int));
        }

        public void Truncate(ulong collectionId)
        {
            var count = 0;

            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*"))
            {
                File.Delete(file);
                count++;
            }

            _keys.Remove(collectionId, out _);

            Log($"truncated collection {collectionId} ({count} files)");
        }

        public void TruncateIndex(ulong collectionId)
        {
            var count = 0;

            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            Log($"truncated index {collectionId} ({count} files)");
        }

        public void Optimize(
            string collection,
            HashSet<string> storeFields, 
            HashSet<string> indexFields,
            ITextModel model,
            int skip = 0,
            int take = 0,
            int batchSize = 1000000)
        {
            var collectionId = collection.ToHash();
            var totalCount = 0;

            TruncateIndex(collectionId);

            using (var docStream = new DocumentStreamSession(this))
            {
                foreach (var batch in docStream.ReadDocs(
                        collectionId,
                        storeFields,
                        skip,
                        take).Batch(batchSize))
                {
                    var job = new WriteJob(
                        collectionId,
                        batch.Select(dic => 
                            new Document(
                                dic.Select(kvp=>new Field(
                                    kvp.Key, 
                                    kvp.Value, 
                                    index: indexFields.Contains(kvp.Key), 
                                    store: storeFields.Contains(kvp.Key))).ToList())),
                        model);

                    Index(job, ref totalCount);

                    Log($"processed {totalCount} documents");
                }
            }

            Log($"optimized collection {collection}");
        }

        public void SaveAs(
            ulong targetCollectionId, 
            IEnumerable<Document> documents,
            ITextModel model,
            int reportSize = 1000)
        {
            var job = new WriteJob(targetCollectionId, documents, model);

            Write(job, reportSize);
        }

        public void Write(WriteJob job, WriteSession writeSession, IndexSession<string> indexSession, int reportSize = 1000)
        {
            Log($"writing to collection {job.CollectionId}");

            var time = Stopwatch.StartNew();

            var batchNo = 0;
            var count = 0;
            var batchTime = Stopwatch.StartNew();

            foreach (var document in job.Documents)
            {
                writeSession.Put(document);

                //Parallel.ForEach(document, kv =>
                foreach (var field in document.Fields)
                {
                    if (field.Value != null && field.Index)
                    {
                        indexSession.Put(document.Id, field.Id, field.Value.ToString());
                    }
                }//);

                if (count++ == reportSize)
                {
                    var info = indexSession.GetIndexInfo();
                    var t = batchTime.Elapsed.TotalSeconds;
                    var docsPerSecond = (int)(reportSize / t);
                    var debug = string.Join('\n', info.Info.Select(x => x.ToString()));

                    Log($"\n{time.Elapsed}\nbatch {++batchNo}\n{debug}\n{docsPerSecond} docs/s");

                    count = 0;
                    batchTime.Restart();
                }
            }

            Logger.LogInformation($"processed write job (collection {job.CollectionId}), time in total: {time.Elapsed}");
        }

        public void Write(
            Document document, 
            WriteSession writeSession, 
            IndexSession<string> indexSession)
        {
            writeSession.Put(document);

            foreach (var field in document.Fields)
            {
                if (field.Value != null && field.Index)
                {
                    indexSession.Put(document.Id, field.Id, field.Value.ToString());
                }
            }
        }

        public void Index(WriteJob job, ref int totalCount, int reportSize = 1000)
        {
            Log($"indexing collection {job.CollectionId}");

            var time = Stopwatch.StartNew();
            var batchTime = Stopwatch.StartNew();
            var batchNo = 0;
            var batchCount = 0;

            using (var indexSession = CreateIndexSession(job.Model))
            {
                foreach (var document in job.Documents)
                {
                    var docId = (long)document.Get(SystemFields.DocumentId).Value;

                    foreach (var field in document.Fields)
                    {
                        if (field.Value != null && field.Index)
                        {
                            indexSession.Put(docId, field.Id, field.Value.ToString());
                        }
                    }

                    if (batchCount++ == reportSize)
                    {
                        var info = indexSession.GetIndexInfo();
                        var t = batchTime.Elapsed.TotalMilliseconds;
                        var docsPerSecond = (int)(reportSize / t * 1000);
                        var debug = string.Join('\n', info.Info.Select(x => x.ToString()));

                        Log($"\n{time.Elapsed}\nbatch {++batchNo}\n{debug}\n{docsPerSecond} docs/s \ntotal {totalCount} docs");

                        batchTime.Restart();
                        totalCount += batchCount;
                        batchCount = 0;
                    }
                }

                using (var stream = new IndexFileStreamProvider(job.CollectionId, this, logger: Logger))
                {
                    stream.Write(indexSession.InMemoryIndex);
                }
            }

            Log($"processed write job (collection {job.CollectionId}), time in total: {time.Elapsed}");
        }

        public void Write(WriteJob job, int reportSize = 1000)
        {
            using (var writeSession = CreateWriteSession(job.CollectionId))
            using (var indexSession = CreateIndexSession(job.Model))
            {
                Write(job, writeSession, indexSession, reportSize);

                using (var stream = new IndexFileStreamProvider(job.CollectionId, this, logger: Logger))
                {
                    stream.Write(indexSession.InMemoryIndex);
                }
            }
        }

        public void Write(
            IEnumerable<IDictionary<string, object>> documents, 
            ITextModel model, 
            HashSet<string> storeFields,
            HashSet<string> indexFields,
            int reportSize = 1000
            )
        {
            foreach (var group in documents.GroupBy(d => (string)d[SystemFields.CollectionId]))
            {
                var collectionId = group.Key.ToHash();

                using (var writeSession = CreateWriteSession(collectionId))
                using (var indexSession = CreateIndexSession(model))
                {
                    Write(
                        new WriteJob(
                            collectionId, 
                            group
                            .Select(dic =>
                                new Document(
                                    dic.Select(kvp => new Field(
                                        kvp.Key,
                                        kvp.Value,
                                        index: indexFields.Contains(kvp.Key),
                                        store: storeFields.Contains(kvp.Key))).ToList())), 
                            model), 
                        writeSession, 
                        indexSession,
                        reportSize);

                    using (var stream = new IndexFileStreamProvider(collectionId, this, logger: Logger))
                    {
                        stream.Write(indexSession.InMemoryIndex);
                    }
                }
            }
        }

        public FileStream CreateLockFile(ulong collectionId)
        {
            return new FileStream(Path.Combine(Directory, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        public void RefreshKeys()
        {
            _keys = LoadKeys();
        }

        public ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> LoadKeys()
        {
            var timer = Stopwatch.StartNew();
            var allkeys = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();

            if (Directory == null)
            {
                return allkeys;
            }

            foreach (var keyFile in System.IO.Directory.GetFiles(Directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                ConcurrentDictionary<ulong, long> keys;

                if (!allkeys.TryGetValue(collectionId, out keys))
                {
                    keys = new ConcurrentDictionary<ulong, long>();
                    allkeys.GetOrAdd(collectionId, keys);
                }

                using (var stream = new FileStream(keyFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    long i = 0;
                    var buf = new byte[sizeof(ulong)];
                    var read = stream.Read(buf, 0, buf.Length);

                    while (read > 0)
                    {
                        keys.GetOrAdd(BitConverter.ToUInt64(buf, 0), i++);

                        read = stream.Read(buf, 0, buf.Length);
                    }
                }
            }

            Log($"loaded keyHash -> keyId mappings into memory for {allkeys.Count} collections in {timer.Elapsed}");

            return allkeys;
        }

        public void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId)
        {
            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(collectionId, out keys))
            {
                keys = new ConcurrentDictionary<ulong, long>();
                _keys.GetOrAdd(collectionId, keys);
            }

            if (!keys.ContainsKey(keyHash))
            {
                keys.GetOrAdd(keyHash, keyId);

                using (var stream = CreateAppendStream(collectionId, "kmap"))
                {
                    stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                }
            }
        }

        public long GetKeyId(ulong collectionId, ulong keyHash)
        {
            return _keys[collectionId][keyHash];
        }

        public bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId)
        {
            var keys = _keys.GetOrAdd(collectionId, new ConcurrentDictionary<ulong, long>());

            if (!keys.TryGetValue(keyHash, out keyId))
            {
                keyId = -1;
                return false;
            }

            return true;
        }
        
        public DocumentStreamSession CreateDocumentStreamSession()
        {
            return new DocumentStreamSession(this);
        }

        public WriteSession CreateWriteSession(ulong collectionId)
        {
            var documentWriter = new DocumentWriter(collectionId, this);

            return new WriteSession(
                collectionId,
                documentWriter
            );
        }

        public IndexSession<string> CreateIndexSession(ITextModel model)
        {
            return new IndexSession<string>(model, model);
        }

        public IndexSession<IImage> CreateIndexSession(IImageModel model)
        {
            return new IndexSession<IImage>(model, model);
        }

        public IQuerySession CreateQuerySession(IModel model)
        {
            return new SearchSession(
                this,
                model,
                new PostingsReader(this),
                Logger);
        }

        public Stream CreateAsyncReadStream(string fileName)
        {
            return File.Exists(fileName)
            ? new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous)
            : null;
        }

        public Stream CreateReadStream(string fileName)
        {
            return File.Exists(fileName)
                ? new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
                : null;
        }

        public Stream CreateAsyncAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        }

        public Stream CreateAppendStream(ulong collectionId, string fileExtension)
        {
            var fileName = Path.Combine(Directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateAppendStream(ulong collectionId, long keyId, string fileExtension)
        {
            var fileName = Path.Combine(Directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public bool CollectionExists(ulong collectionId)
        {
            return File.Exists(Path.Combine(Directory, collectionId + ".vec"));
        }

        public bool CollectionIsIndexOnly(ulong collectionId)
        {
            if (!CollectionExists(collectionId))
                throw new InvalidOperationException($"{collectionId} dows not exist");

            return !File.Exists(Path.Combine(Directory, collectionId + ".docs"));
        }

        public void Dispose()
        {
        }
    }
}