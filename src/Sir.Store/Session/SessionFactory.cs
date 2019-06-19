﻿using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;

namespace Sir.Store
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ILogger
    {
        private readonly IConfigurationProvider _config;
        private readonly ConcurrentDictionary<string, MemoryMappedFile> _mmfs;
        private readonly IStringModel _model;
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;
        private readonly ConcurrentDictionary<string, IList<(long offset, long length)>> _pageInfo;
        private readonly ConcurrentDictionary<string, VectorNode> _graph;
        private static readonly object WriteSync = new object();
        private bool _isInitialized;

        public string Dir { get; }
        public IConfigurationProvider Config { get { return _config; } }

        public SessionFactory(IConfigurationProvider config, IStringModel model)
        {
            Dir = config.Get("data_dir");

            if (!Directory.Exists(Dir))
            {
                Directory.CreateDirectory(Dir);
            }

            _model = model;
            _keys = LoadKeys();
            _config = config;
            _pageInfo = new ConcurrentDictionary<string, IList<(long offset, long length)>>();
            _mmfs = new ConcurrentDictionary<string, MemoryMappedFile>();
            _graph = new ConcurrentDictionary<string, VectorNode>();

            this.Log($"initiated");
        }

        private void LoadGraph()
        {
            _isInitialized = false;

            var gtimer = Stopwatch.StartNew();

            Parallel.ForEach(Directory.GetFiles(Dir, "*.ix"), fileName =>
            //foreach (var fileName in Directory.GetFiles(Dir, "*.ix"))
            {
                var ftimer = Stopwatch.StartNew();
                var pageFileName = Path.Combine(Dir, $"{Path.GetFileNameWithoutExtension(fileName)}.ixp");
                var vectorFileName = Path.Combine(Dir, $"{Path.GetFileNameWithoutExtension(fileName)}.vec");
                var ixFile = OpenMMF(fileName);
                var vecFile = OpenMMF(vectorFileName);
                var root = _graph.GetOrAdd(fileName, new VectorNode());

                Parallel.ForEach(ReadPageInfo(pageFileName), page =>
                //foreach (var page in ReadPageInfo(pageFileName))
                {
                    var timer = Stopwatch.StartNew();

                    using (var vectorView = vecFile.CreateViewAccessor(0, 0))
                    using (var indexView = ixFile.CreateViewAccessor(page.offset, page.length))
                    {
                        try
                        {
                            var length = page.length / VectorNode.BlockSize;
                            var buf = new VectorNodeData[length];
                            var read = indexView.ReadArray(0, buf, 0, buf.Length);

                            foreach (var item in buf)
                            {
                                var vector = _model.DeserializeVector(
                                    item.VectorOffset, (int)item.ComponentCount, vectorView);

                                GraphBuilder.Add(
                                    root, new VectorNode(vector, new List<long> { item.PostingsOffset }), _model);
                            }
                        }
                        catch (Exception ex)
                        {
                            this.Log(ex.ToString());

                            throw;
                        }
                    }

                    this.Log($"loaded page {page} from {fileName} into memory in {timer.Elapsed}");
                });

                this.Log($"{fileName} fully loaded into memory in {ftimer.Elapsed}");
            });

            this.Log($"graph fully loaded into memory in {gtimer.Elapsed}");

            _isInitialized = true;
        }

        private ConcurrentDictionary<string, ConcurrentDictionary<long, IMemoryOwner<VectorNodeData>>> LoadIndexMemory()
        {
            var indexMemory = new ConcurrentDictionary<string, ConcurrentDictionary<long, IMemoryOwner<VectorNodeData>>>();

            Parallel.ForEach(Directory.GetFiles(Dir, "*.ix"), fileName =>
            //foreach (var fileName in Directory.GetFiles(Dir, "*.ix"))
            {
                var pageFileName = Path.Combine(Dir, $"{Path.GetFileNameWithoutExtension(fileName)}.ixp");
                var indexFile = OpenMMF(fileName);
                var pages = indexMemory.GetOrAdd(fileName, new ConcurrentDictionary<long, IMemoryOwner<VectorNodeData>>());

                Parallel.ForEach(ReadPageInfo(pageFileName), page =>
                //foreach (var page in ReadPageInfo(pageFileName))
                {
                    var timer = Stopwatch.StartNew();

                    using (var indexView = indexFile.CreateViewAccessor(page.offset, page.length))
                    {
                        try
                        {
                            var length = page.length / VectorNode.BlockSize;
                            var buf = new VectorNodeData[length];
                            var read = indexView.ReadArray(0, buf, 0, buf.Length);
                            IMemoryOwner<VectorNodeData> owner = MemoryPool<VectorNodeData>.Shared.Rent(minBufferSize:buf.Length);
                            buf.AsSpan().CopyTo(owner.Memory.Span);
                            pages.GetOrAdd(page.offset, owner);
                        }
                        catch (Exception ex)
                        {
                            this.Log(ex.ToString());

                            throw;
                        }
                    }

                    this.Log($"loaded page {page} from {fileName} into memory in {timer.Elapsed}");
                });
            });

            return indexMemory;
        }

        public void BeginInit()
        {
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;

                try
                {
                    LoadGraph();

                }
                catch (Exception ex)
                {
                    this.Log(ex.ToString());
                }
            }).Start();
        }

        public VectorNode GetGraph(string ixFileName)
        {
            if (!_isInitialized)
                throw new InvalidOperationException(
                    "Unable to respond while initializing. Check log to see progress of init.");

            return _graph[ixFileName];
        }

        public MemoryMappedFile OpenMMF(string fileName)
        {
            var mapName = fileName.Replace(":", "").Replace("\\", "_");

            return _mmfs.GetOrAdd(mapName, x =>
            {
                return MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, mapName, 0, MemoryMappedFileAccess.ReadWrite);
            });
        }

        public void Truncate(ulong collectionId)
        {
            foreach (var file in Directory.GetFiles(Dir, $"{collectionId}*"))
            {
                File.Delete(file);
            }

            _pageInfo.Clear();

            _keys.Clear();
        }

        public long ExecuteWrite(string collectionName, IStringModel model, IDictionary<string, object> document)
        {
            lock (WriteSync)
            {
                var timer = Stopwatch.StartNew();
                var colId = collectionName.ToHash();
                long docId;

                using (var indexSession = CreateIndexSession(collectionName, colId))
                using (var writeSession = CreateWriteSession(collectionName, colId, indexSession))
                {
                    docId = writeSession.Write(document);

                    writeSession.Commit();
                }

                _pageInfo.Clear();

                this.Log("executed {0} write+index job in {1}", collectionName, timer.Elapsed);

                return docId;
            }
        }

        public void ExecuteWrite(string collectionName, IStringModel model, IEnumerable<IDictionary<string, object>> documents)
        {
            lock (WriteSync)
            {
                var timer = Stopwatch.StartNew();
                var colId = collectionName.ToHash();

                using (var indexSession = CreateIndexSession(collectionName, colId))
                using (var writeSession = CreateWriteSession(collectionName, colId, indexSession))
                {
                    foreach (var document in documents)
                    {
                        writeSession.Write(document);
                    }
                    writeSession.Commit();
                }

                _pageInfo.Clear();

                this.Log("executed {0} write+index job in {1}", collectionName, timer.Elapsed);
            }
        }

        public IList<(long offset, long length)> ReadPageInfo(string pageFileName)
        {
            return _pageInfo.GetOrAdd(pageFileName, key =>
            {
                using (var ixpStream = CreateReadStream(key))
                {
                    return new PageIndexReader(ixpStream).ReadAll();
                }
            });
        }

        public void RefreshKeys()
        {
            _keys = LoadKeys();
        }

        public ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> LoadKeys()
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
            return new WarmupSession(collectionName, collectionId, this, _model, _config, baseUrl);
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
            return new TermIndexSession(collectionName, collectionId, this, _model, _config);
        }

        public ValidateSession CreateValidateSession(string collectionName, ulong collectionId)
        {
            return new ValidateSession(collectionName, collectionId, this, _model, _config);
        }

        public ReadSession CreateReadSession(string collectionName, ulong collectionId)
        {
            return new ReadSession(collectionName, collectionId, this, _config, _model);
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
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, true);
        }

        public Stream CreateAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.Read);
        }

        public bool CollectionExists(ulong collectionId)
        {
            return File.Exists(Path.Combine(Dir, collectionId + ".val"));
        }

        public void Dispose()
        {
            foreach(var x in _mmfs)
            {
                x.Value.Dispose();
            }
        }
    }
}