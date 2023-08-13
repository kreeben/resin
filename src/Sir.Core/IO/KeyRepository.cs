using System.Collections.Concurrent;
using System;
using System.IO;
using System.Collections.Generic;

namespace Sir.IO
{
    public class KeyRepository
    {
        private readonly string _directory;
        private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;
        private readonly ISessionFactory _sessionFactory;

        public KeyRepository()
        {
            _keys = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();
        }

        public KeyRepository(string directory, ISessionFactory sessionFactory)
        {
            _directory = directory;
            _sessionFactory = sessionFactory;
        }

        public void RemoveFromCache(ulong key)
        {
            _keys.Remove(key, out _);
        }

        public void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId)
        {
            var keys = _keys.GetOrAdd(collectionId, (key) => { return new ConcurrentDictionary<ulong, long>(); });
            var keyMapping = keys.GetOrAdd(keyHash, (key) =>
            {
                if (!string.IsNullOrWhiteSpace(_directory))
                {
                    using (var stream = _sessionFactory.CreateAppendStream(_directory, collectionId, "kmap"))
                    {
                        stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                    }
                }
                
                return keyId;
            });
        }

        public long GetKeyId(ulong collectionId, ulong keyHash)
        {
            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(collectionId, out keys))
            {
                ReadKeysIntoCache();
            }

            if (keys != null || _keys.TryGetValue(collectionId, out keys))
            {
                return keys[keyHash];
            }

            throw new Exception($"unable to find key {keyHash} for collection {collectionId} in directory {_directory}.");
        }

        public bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId)
        {
            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(collectionId, out keys))
            {
                ReadKeysIntoCache();
            }

            if (keys != null || _keys.TryGetValue(collectionId, out keys))
            {
                if (keys.TryGetValue(keyHash, out keyId))
                {
                    return true;
                }
            }

            keyId = -1;
            return false;
        }

        private void ReadKeysIntoCache()
        {
            if (string.IsNullOrWhiteSpace(_directory))
                return;

            foreach (var keyFile in Directory.GetFiles(_directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                var key = Path.Combine(_directory, collectionId.ToString()).ToHash();

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
    }
}