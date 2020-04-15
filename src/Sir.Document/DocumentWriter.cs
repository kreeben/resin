using Sir.KeyValue;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.Document
{
    public class DocumentWriter : IDisposable
    {
        private readonly ValueWriter _vals;
        private readonly ValueWriter _keys;
        private readonly DocMapWriter _docs;
        private readonly ValueIndexWriter _valIx;
        private readonly ValueIndexWriter _keyIx;
        private readonly DocIndexWriter _docIx;
        private readonly ulong _collectionId;
        private readonly ISessionFactory _sessionFactory;
        private readonly object _keyLock = new object();
        
        public DocumentWriter(ulong collectionId, ISessionFactory sessionFactory)
        {
            var valueStream = sessionFactory.CreateAppendStream(
                Path.Combine(
                    sessionFactory.Dir, 
                    string.Format("{0}.val", collectionId)), 
                int.Parse(sessionFactory.Config.Get("value_stream_write_buffer_size")));

            var keyStream = sessionFactory.CreateAppendStream(
                Path.Combine(
                    sessionFactory.Dir, 
                    string.Format("{0}.key", collectionId)));

            var docStream = sessionFactory.CreateAppendStream(
                Path.Combine(
                    sessionFactory.Dir, 
                    string.Format("{0}.docs", collectionId)), 
                int.Parse(sessionFactory.Config.Get("doc_map_stream_write_buffer_size")));

            var valueIndexStream = sessionFactory.CreateAppendStream(
                Path.Combine(
                    sessionFactory.Dir, 
                    string.Format("{0}.vix", collectionId)));

            var keyIndexStream = sessionFactory.CreateAppendStream(
                Path.Combine(
                    sessionFactory.Dir, 
                    string.Format("{0}.kix", collectionId)));

            var docIndexStream = sessionFactory.CreateAppendStream(
                Path.Combine(
                    sessionFactory.Dir, 
                    string.Format("{0}.dix", collectionId)));

            _vals = new ValueWriter(valueStream);
            _keys = new ValueWriter(keyStream);
            _docs = new DocMapWriter(docStream);
            _valIx = new ValueIndexWriter(valueIndexStream);
            _keyIx = new ValueIndexWriter(keyIndexStream);
            _docIx = new DocIndexWriter(docIndexStream);
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
        }

        public long EnsureKeyExists(string keyStr)
        {
            var keyHash = keyStr.ToHash();
            long keyId;

            if (!_sessionFactory.TryGetKeyId(_collectionId, keyHash, out keyId))
            {
                lock (_keyLock)
                {
                    if (!_sessionFactory.TryGetKeyId(_collectionId, keyHash, out keyId))
                    {
                        // We have a new key!

                        // store key
                        var keyInfo = PutKey(keyStr);

                        keyId = PutKeyInfo(keyInfo.offset, keyInfo.len, keyInfo.dataType);

                        // store key mapping
                        _sessionFactory.RegisterKeyMapping(_collectionId, keyHash, keyId);
                    }
                }
            }

            return keyId;
        }

        public (long keyId, long valueId) Put(long keyId, object val, out byte dataType)
        {
            // store value
            var valInfo = PutValue(val);
            var valId = PutValueInfo(valInfo.offset, valInfo.len, valInfo.dataType);

            dataType = valInfo.dataType;

            // return refs to key and value
            return (keyId, valId);
        }

        public long IncrementDocId()
        {
            return _docIx.IncrementDocId();
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

        public (long offset, int length) PutDocumentMap(IList<(long keyId, long valId)> doc)
        {
            return _docs.Put(doc);
        }

        public void PutDocumentAddress(long id, long offset, int len)
        {
            _docIx.Put(id, offset, len);
        }

        public void Flush()
        {
            _vals.Flush();
            _keys.Flush();
            _docs.Flush();
            _valIx.Flush();
            _keyIx.Flush();
            _docIx.Flush();
        }

        public void Dispose()
        {
            _vals.Dispose();
            _keys.Dispose();
            _docs.Dispose();
            _valIx.Dispose();
            _keyIx.Dispose();
            _docIx.Dispose();
        }
    }
}
