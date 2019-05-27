using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Store
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ILogger
    {
        private readonly ITokenizer _tokenizer;
        private readonly IConfigurationProvider _config;
        private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;
        private readonly ConcurrentDictionary<string, IList<(long offset, long length)>> _pageInfo;

        public string Dir { get; }
        public IConfigurationProvider Config { get { return _config; } }

        public SessionFactory(ITokenizer tokenizer, IConfigurationProvider config)
        {
            Dir = config.Get("data_dir");

            if (!Directory.Exists(Dir))
            {
                Directory.CreateDirectory(Dir);
            }

            _keys = LoadKeys();
            _tokenizer = tokenizer;
            _config = config;
            _pageInfo = new ConcurrentDictionary<string, IList<(long offset, long length)>>();
        }

        public void Truncate(ulong collectionId)
        {
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*"))
            {
                File.Delete(file);
            }
        }

        public void Commit(Job job)
        {
            var timer = Stopwatch.StartNew();
            var colId = job.Collection.ToHash();

            using (var indexSession = CreateIndexSession(job.Collection, colId))
            using (var writeSession = CreateWriteSession(job.Collection, colId, indexSession))
            {
                foreach (var doc in job.Documents)
                {
                    writeSession.Write(doc);
                }

                writeSession.Commit();
            }

            _pageInfo.Clear();

            this.Log("executed {0} write+index job in {1}", job.Collection, timer.Elapsed);
        }

        public IList<(long offset, long length)> ReadPageInfoFromDisk(string ixpFileName)
        {
            return _pageInfo.GetOrAdd(ixpFileName, key =>
            {
                using (var ixpStream = CreateReadStream(key))
                {
                    return new PageIndexReader(ixpStream).ReadAll();
                }
            });
        }

        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> LoadKeys()
        {
            var timer = new Stopwatch();
            timer.Start();

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

            this.Log("loaded keys into memory in {0}", timer.Elapsed);

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

        public WarmupSession CreateWarmupSession(string collectionName, ulong collectionId, string baseUrl)
        {
            return new WarmupSession(collectionName, collectionId, this, _tokenizer, _config, baseUrl);
        }

        public DocumentStreamSession CreateDocumentStreamSession(string collectionName, ulong collectionId)
        {
            return new DocumentStreamSession(collectionName, collectionId, this);
        }

        public WriteSession CreateWriteSession(string collectionName, ulong collectionId, TermIndexSession indexSession)
        {
            return new WriteSession(
                collectionName, collectionId, this, indexSession, _config);
        }

        public TermIndexSession CreateIndexSession(string collectionName, ulong collectionId)
        {
            return new TermIndexSession(collectionName, collectionId, this, _tokenizer, _config);
        }

        public BowIndexSession CreateBOWSession(string collectionName, ulong collectionId)
        {
            return new BowIndexSession(collectionName, collectionId, this, _config, _tokenizer);
        }

        public ValidateSession CreateValidateSession(string collectionName, ulong collectionId)
        {
            return new ValidateSession(collectionName, collectionId, this, _tokenizer, _config);
        }

        public ReadSession CreateReadSession(string collectionName, ulong collectionId)
        {
            return new ReadSession(collectionName, collectionId, this, _config);
        }

        public Stream CreateAsyncReadStream(string fileName)
        {
            return File.Exists(fileName)
            ? new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, true)
            : null;
        }

        public Stream CreateReadStream(string fileName, FileOptions fileOptions = FileOptions.RandomAccess)
        {
            return File.Exists(fileName)
                ? new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, fileOptions)
                : null;
        }

        public Stream CreateAsyncAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 4096, true);
        }

        public Stream CreateAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public bool CollectionExists(ulong collectionId)
        {
            return File.Exists(Path.Combine(Dir, collectionId + ".val"));
        }

        public void Dispose()
        {
        }
    }
}