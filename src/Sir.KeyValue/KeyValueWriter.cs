using System;

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
        private readonly KeyValueReader _kvReader;
        private static object _keyLock = new object();

        public KeyValueWriter(string directory, ulong collectionId)
            : this(
                new ValueWriter(StreamFactory.CreateAppendStream(directory, collectionId, "val")),
                new ValueWriter(StreamFactory.CreateAppendStream(directory, collectionId, "key")),
                new ValueIndexWriter(StreamFactory.CreateAppendStream(directory, collectionId, "vix")),
                new ValueIndexWriter(StreamFactory.CreateAppendStream(directory, collectionId, "kix"))
                )
        {
            _collectionId = collectionId;
            _directory = directory;
            _kvReader = new KeyValueReader(directory, collectionId);
        }

        public KeyValueWriter(ValueWriter values, ValueWriter keys, ValueIndexWriter valIx, ValueIndexWriter keyIx)
        {
            _vals = values;
            _keys = keys;
            _valIx = valIx;
            _keyIx = keyIx;
        }

        public long EnsureKeyExists(string keyStr)
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
                        using (var stream = StreamFactory.CreateAppendStream(_directory, _collectionId, "kmap"))
                        {
                            stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                        }
                    }
                }
            }

            return keyId;
        }

        public (long keyId, long valueId, byte dataType) PutValue(long keyId, object value)
        {
            // store value
            var valInfo = PutValue(value);
            var valId = PutValueInfo(valInfo.offset, valInfo.len, valInfo.dataType);

            // return refs to key and value
            return (keyId, valId, valInfo.dataType);
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

        public void Dispose()
        {
            _vals.Dispose();
            _keys.Dispose();
            _valIx.Dispose();
            _keyIx.Dispose();
            _kvReader.Dispose();
        }
    }
}
