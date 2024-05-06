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
        private readonly KeyValueReader _kvReader;

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
            _kvReader = new KeyValueReader(directory, collectionId);
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

            if (!_kvReader.TryGetKeyId(keyHash, out keyId))
            {
                lock (_keyLock)
                {
                    if (!_kvReader.TryGetKeyId(keyHash, out keyId))
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

            if (!_kvReader.TryGetKeyId(keyHash, out keyId))
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

        public virtual void Dispose()
        {
            _vals.Dispose();
            _keys.Dispose();
            _valIx.Dispose();
            _keyIx.Dispose();
            _kvReader.Dispose();
        }
    }
}
