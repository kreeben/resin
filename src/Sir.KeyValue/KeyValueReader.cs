using System;
using System.Collections.Concurrent;
using System.IO;

namespace Sir.KeyValue
{
    public class KeyValueReader : IDisposable
    {
        private readonly ulong _collectionId;
        private readonly string _directory;
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keyCache;

        public KeyValueReader(string directory, ulong collectionId)
        {
            _collectionId = collectionId;
            _directory = directory;
            _keyCache = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();
        }

        public long GetKeyId(ulong keyHash)
        {
            var key = Path.Combine(_directory, _collectionId.ToString()).ToHash();

            ConcurrentDictionary<ulong, long> keys;

            if (!_keyCache.TryGetValue(key, out keys))
            {
                ReadKeysIntoCache();
            }

            if (keys != null || _keyCache.TryGetValue(key, out keys))
            {
                return keys[keyHash];
            }

            throw new Exception($"unable to find key {keyHash} for collection {_collectionId} in directory {_directory}.");
        }

        public bool TryGetKeyId(ulong keyHash, out long keyId)
        {
            var key = Path.Combine(_directory, _collectionId.ToString()).ToHash();

            ConcurrentDictionary<ulong, long> keys;

            if (!_keyCache.TryGetValue(key, out keys))
            {
                ReadKeysIntoCache();
            }

            if (keys != null || _keyCache.TryGetValue(key, out keys))
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
            foreach (var keyFile in Directory.GetFiles(_directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                var key = Path.Combine(_directory, collectionId.ToString()).ToHash();

                var keys = _keyCache.GetOrAdd(key, (k) =>
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

        public virtual void Dispose()
        {
        }
    }
}
