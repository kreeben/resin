using System;
using System.Collections.Concurrent;
using System.IO;

namespace Sir.KeyValue
{
    /// <summary>
    /// Writes keys and values to a database.
    /// </summary>
    public class KeyValueWriter : IDisposable
    {
        private readonly ValueWriter _vals;
        private readonly ValueWriter _keys;
        private readonly ValueIndexWriter _valIx;
        private readonly ValueIndexWriter _keyIx;
        private readonly ulong _collectionId;
        private readonly string _directory;
        private readonly object _keyLock = new object();
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keyCache;

        public KeyValueWriter(string directory, ulong collectionId)
            : this(
                new ValueWriter(CreateAppendStream(directory, collectionId, "val")),
                new ValueWriter(CreateAppendStream(directory, collectionId, "key")),
                new ValueIndexWriter(CreateAppendStream(directory, collectionId, "vix")),
                new ValueIndexWriter(CreateAppendStream(directory, collectionId, "kix"))
                )
        {
            _collectionId = collectionId;
            _directory = directory;
            _keyCache = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();
        }

        public KeyValueWriter(ValueWriter values, ValueWriter keys, ValueIndexWriter valIx, ValueIndexWriter keyIx)
        {
            _vals = values;
            _keys = keys;
            _valIx = valIx;
            _keyIx = keyIx;
        }

        public static Stream CreateAppendStream(string directory, ulong collectionId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public static Stream CreateAppendStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public long EnsureKeyExistsSafely(string keyStr)
        {
            var keyHash = keyStr.ToHash();
            long keyId;

            if (!TryGetKeyId(_directory, _collectionId, keyHash, out keyId))
            {
                lock (_keyLock)
                {
                    if (!TryGetKeyId(_directory, _collectionId, keyHash, out keyId))
                    {
                        // We have a new key!

                        // store key
                        var keyInfo = PutKey(keyStr);

                        keyId = PutKeyInfo(keyInfo.offset, keyInfo.len, keyInfo.dataType);

                        // store key mapping
                        RegisterKeyMapping(_directory, _collectionId, keyHash, keyId);
                    }
                }
            }

            return keyId;
        }

        public long EnsureKeyExists(string keyStr)
        {
            var keyHash = keyStr.ToHash();
            long keyId;

            if (!TryGetKeyId(_directory, _collectionId, keyHash, out keyId))
            {
                // We have a new key!

                // store key
                var keyInfo = PutKey(keyStr);

                keyId = PutKeyInfo(keyInfo.offset, keyInfo.len, keyInfo.dataType);

                // store key mapping
                RegisterKeyMapping(_directory, _collectionId, keyHash, keyId);
            }

            return keyId;
        }

        public (long keyId, long valueId) PutValue(long keyId, object val, out byte dataType)
        {
            // store value
            var valInfo = PutValue(val);
            var valId = PutValueInfo(valInfo.offset, valInfo.len, valInfo.dataType);

            dataType = valInfo.dataType;

            // return refs to key and value
            return (keyId, valId);
        }

        public (long offset, int len, byte dataType) PutKey(object value)
        {
            return _keys.Put(value);
        }

        public (long offset, int len, byte dataType) PutValue(object value)
        {
            return _vals.Put(value);
        }

        public long PutKeyInfo(long offset, int len, byte dataType)
        {
            return _keyIx.Put(offset, len, dataType);
        }

        public long PutValueInfo(long offset, int len, byte dataType)
        {
            return _valIx.Put(offset, len, dataType);
        }

        public void OverwriteFixedLengthValue(long offset, object value, Type type)
        {
            if (type == typeof(string) || type == typeof(byte[]))
                throw new InvalidOperationException();

            _vals.Stream.Seek(offset, System.IO.SeekOrigin.Begin);
            _vals.Put(value);
        }

        public void RegisterKeyMapping(string directory, ulong collectionId, ulong keyHash, long keyId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();
            var keys = _keyCache.GetOrAdd(key, (key) => { return new ConcurrentDictionary<ulong, long>(); });
            var keyMapping = keys.GetOrAdd(keyHash, (key) =>
            {
                using (var stream = CreateAppendStream(directory, collectionId, "kmap"))
                {
                    stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                }
                return keyId;
            });
        }

        public long GetKeyId(string directory, ulong collectionId, ulong keyHash)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            ConcurrentDictionary<ulong, long> keys;

            if (!_keyCache.TryGetValue(key, out keys))
            {
                ReadKeysIntoCache(directory);
            }

            if (keys != null || _keyCache.TryGetValue(key, out keys))
            {
                return keys[keyHash];
            }

            throw new Exception($"unable to find key {keyHash} for collection {collectionId} in directory {directory}.");
        }

        public bool TryGetKeyId(string directory, ulong collectionId, ulong keyHash, out long keyId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            ConcurrentDictionary<ulong, long> keys;

            if (!_keyCache.TryGetValue(key, out keys))
            {
                ReadKeysIntoCache(directory);
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

        private void ReadKeysIntoCache(string directory)
        {
            foreach (var keyFile in Directory.GetFiles(directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                var key = Path.Combine(directory, collectionId.ToString()).ToHash();

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
            _vals.Dispose();
            _keys.Dispose();
            _valIx.Dispose();
            _keyIx.Dispose();
        }
    }
}
