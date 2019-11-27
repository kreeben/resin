using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Document;
using Sir.KeyValue;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sir.Search
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ISessionFactory
    {
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;
        private readonly ConcurrentDictionary<string, IList<(long offset, long length)>> _pageInfo;
        private readonly ConcurrentDictionary<string, MemoryMappedFile> _mmfs;
        private ILogger<SessionFactory> _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly Semaphore _semaphore = new Semaphore(1, 2);

        public string Dir { get; }
        public IConfigurationProvider Config { get; }
        public IStringModel Model { get; }

        public SessionFactory(IConfigurationProvider config, IStringModel model, ILoggerFactory loggerFactory)
        {
            var time = Stopwatch.StartNew();

            Dir = config.Get("data_dir");
            Config = config;

            if (!Directory.Exists(Dir))
            {
                Directory.CreateDirectory(Dir);
            }

            _pageInfo = new ConcurrentDictionary<string, IList<(long offset, long length)>>();
            _mmfs = new ConcurrentDictionary<string, MemoryMappedFile>();
            Model = model;

            _logger = loggerFactory.CreateLogger<SessionFactory>();
            _loggerFactory = loggerFactory;

            _keys = LoadKeys();

            _logger.LogInformation($"initiated in {time.Elapsed}");
        }

        public MemoryMappedFile OpenMMF(string fileName)
        {
            var mapName = fileName.Replace(":", "").Replace("\\", "_");

            try
            {
                return _mmfs.GetOrAdd(mapName, x =>
                {
                    return MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, mapName, 0, MemoryMappedFileAccess.ReadWrite);
                });
            }
            catch
            {
                return _mmfs.GetOrAdd(mapName, x =>
                {
                    return MemoryMappedFile.OpenExisting(mapName);
                });
            }
        }

        public long GetDocCount(string collection)
        {
            var fileName = Path.Combine(Dir, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / (sizeof(long) + sizeof(int));
        }

        public void Truncate(ulong collectionId)
        {
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*"))
                File.Delete(file);

            _pageInfo.Clear();

            _keys.Clear();

            _logger.LogInformation($"truncated {collectionId}");
        }

        public void TruncateIndex(ulong collectionId)
        {
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.ix"))
            {
                File.Delete(file);
            }
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
            }
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.vec"))
            {
                File.Delete(file);
            }
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*.pos"))
            {
                File.Delete(file);
            }

            _pageInfo.Clear();
        }

        public IndexInfo Write(Job job, WriteSession writeSession, IndexSession indexSession)
        {
            var payload = new List<IDictionary<string, object>>(job.Documents);

            foreach (var document in payload)
            {
                writeSession.Write(document);
            }

            //Parallel.ForEach(payload, document =>
            foreach (var document in payload)
            {
                var docId = (long)document["___docid"];

                //Parallel.ForEach(document, kv =>
                foreach (var kv in document)
                {
                    if (!kv.Key.StartsWith("_"))
                    {
                        var keyId = GetKeyId(job.CollectionId, kv.Key.ToHash());

                        indexSession.Put(docId, keyId, kv.Value);
                    }
                }//);
            }//);

            return indexSession.GetIndexInfo();
        }

        public void WriteAndReport(Job job, WriteSession writeSession, IndexSession indexSession, ILogger logger, int reportSize)
        {
            var time = Stopwatch.StartNew();
            var info = Write(job, writeSession, indexSession);
            var t = time.Elapsed.TotalMilliseconds;
            var docsPerSecond = (int)(reportSize / t * 1000);
            var debug = string.Join('\n', info.Info.Select(x => x.ToString()));

            logger.LogInformation($"{debug}\n{docsPerSecond} docs/s\n");
        }

        public void WriteAndReportConcurrent(Job job, ILogger logger, int reportSize = 1000)
        {
            _semaphore.WaitOne();

            var batchNo = 0;

            using (var writeSession = CreateWriteSession(job.CollectionId))
            using (var indexSession = CreateIndexSession(job.CollectionId))
            {
                foreach (var batch in job.Documents.Batch(reportSize))
                {
                    WriteAndReport(
                            new Job(job.CollectionId, batch, job.Model),
                            writeSession,
                            indexSession,
                            logger,
                            reportSize);

                    logger.LogInformation($"processed batch {++batchNo}");
                }
            }

            _semaphore.Release();
        }

        public void WriteConcurrent(Job job)
        {
            _semaphore.WaitOne();

            using (var writeSession = CreateWriteSession(job.CollectionId))
            using (var indexSession = CreateIndexSession(job.CollectionId))
            {
                Write(job, writeSession, indexSession);
            }

            _semaphore.Release();
        }

        public void WriteConcurrent(Job job, IndexSession indexSession)
        {
            _semaphore.WaitOne();

            using (var writeSession = CreateWriteSession(job.CollectionId))
            {
                Write(job, writeSession, indexSession);
            }

            _semaphore.Release();
        }

        public void WriteConcurrent(IEnumerable<IDictionary<string, object>> documents, IStringModel model)
        {
            _semaphore.WaitOne();

            Parallel.ForEach(documents.GroupBy(d => (string)d["___collectionid"]), group =>
            //foreach (var group in documents.GroupBy(d => (string)d["___collectionid"]))
            {
                var collectionId = group.Key.ToHash();

                using (var writeSession = CreateWriteSession(collectionId))
                using (var indexSession = CreateIndexSession(collectionId))
                {
                    Write(new Job(collectionId, group, model), writeSession, indexSession);
                }
            });

            _semaphore.Release();
        }

        public FileStream CreateLockFile(ulong collectionId)
        {
            return new FileStream(Path.Combine(Dir, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        public void ClearPageInfo()
        {
            _pageInfo.Clear();
        }

        public IList<(long offset, long length)> GetAllPages(string pageFileName)
        {
            return _pageInfo.GetOrAdd(pageFileName, key =>
            {
                using (var ixpStream = CreateReadStream(key))
                {
                    return new PageIndexReader(ixpStream).GetAll();
                }
            });
        }

        public void RefreshKeys()
        {
            _keys = LoadKeys();
        }

        public ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> LoadKeys()
        {
            var timer = Stopwatch.StartNew();
            var allkeys = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();

            foreach (var keyFile in Directory.GetFiles(Dir, "*.kmap"))
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

            _logger.LogInformation($"loaded keyHash -> keyId mappings into memory for {allkeys.Count} collections in {timer.Elapsed}");

            return allkeys;
        }

        public void PersistKeyMapping(ulong collectionId, ulong keyHash, long keyId)
        {
            var fileName = Path.Combine(Dir, string.Format("{0}.kmap", collectionId));
            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(collectionId, out keys))
            {
                keys = new ConcurrentDictionary<ulong, long>();
                _keys.GetOrAdd(collectionId, keys);
            }

            if (!keys.ContainsKey(keyHash))
            {
                keys.GetOrAdd(keyHash, keyId);

                using (var stream = CreateAppendStream(fileName))
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

        public string GetKey(ulong collectionId, long keyId)
        {
            using (var indexReader = new ValueIndexReader(CreateReadStream(Path.Combine(Dir, $"{collectionId}.kix"))))
            using (var reader = new ValueReader(CreateReadStream(Path.Combine(Dir, $"{collectionId}.key"))))
            {
                var keyInfo = indexReader.Get(keyId);

                return (string)reader.Get(keyInfo.offset, keyInfo.len, keyInfo.dataType);
            }
        }

        public DocumentStreamSession CreateDocumentStreamSession(ulong collectionId)
        {
            return new DocumentStreamSession(new DocumentReader(collectionId, this));
        }

        public WriteSession CreateWriteSession(ulong collectionId)
        {
            var documentWriter = new DocumentWriter(collectionId, this);

            return new WriteSession(
                collectionId,
                documentWriter
            );
        }

        public IndexSession CreateIndexSession(ulong collectionId)
        {
            return new IndexSession(collectionId, this, Model, Config, _loggerFactory.CreateLogger<IndexSession>());
        }

        public IReadSession CreateReadSession()
        {
            return new ReadSession(
                this,
                Config,
                Model,
                new PostingsReader(this),
                _loggerFactory.CreateLogger<ReadSession>());
        }

        public ValidateSession CreateValidateSession(ulong collectionId)
        {
            return new ValidateSession(
                collectionId,
                this,
                Model,
                Config,
                new PostingsReader(this));
        }

        public Stream CreateAsyncReadStream(string fileName, int bufferSize = 4096)
        {
            return File.Exists(fileName)
            ? new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, FileOptions.Asynchronous)
            : null;
        }

        public Stream CreateReadStream(string fileName, int bufferSize = 4096, FileOptions fileOptions = FileOptions.RandomAccess)
        {
            return File.Exists(fileName)
                ? new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, fileOptions)
                : null;
        }

        public Stream CreateAsyncAppendStream(string fileName, int bufferSize = 4096)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, bufferSize, FileOptions.Asynchronous);
        }

        public Stream CreateAppendStream(string fileName, int bufferSize = 4096)
        {
            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, bufferSize))
                {
                }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, bufferSize);
        }

        public bool CollectionExists(ulong collectionId)
        {
            return File.Exists(Path.Combine(Dir, collectionId + ".docs"));
        }

        public void Dispose()
        {
            foreach (var file in _mmfs.Values)
                file.Dispose();
        }
    }
}