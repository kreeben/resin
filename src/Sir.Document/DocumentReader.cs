using Sir.KeyValue;
using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.Documents
{
    /// <summary>
    /// Read documents from storage.
    /// </summary>
    public class DocumentReader : IDisposable
    {
        private readonly ValueReader _vals;
        private readonly ValueReader _keys;
        private readonly DocMapReader _docs;
        private readonly ValueIndexReader _valIx;
        private readonly ValueIndexReader _keyIx;
        private readonly DocIndexReader _docIx;

        public ulong CollectionId { get; }

        public DocumentReader(string directory, ulong collectionId)
        {
            var valueStream = CreateReadStream(Path.Combine(directory, string.Format("{0}.val", collectionId)));
            var keyStream = CreateReadStream(Path.Combine(directory, string.Format("{0}.key", collectionId)));
            var docStream = CreateReadStream(Path.Combine(directory, string.Format("{0}.docs", collectionId)));
            var valueIndexStream = CreateReadStream(Path.Combine(directory, string.Format("{0}.vix", collectionId)));
            var keyIndexStream = CreateReadStream(Path.Combine(directory, string.Format("{0}.kix", collectionId)));
            var docIndexStream = CreateReadStream(Path.Combine(directory, string.Format("{0}.dix", collectionId)));

            _vals = new ValueReader(valueStream);
            _keys = new ValueReader(keyStream);
            _docs = new DocMapReader(docStream);
            _valIx = new ValueIndexReader(valueIndexStream);
            _keyIx = new ValueIndexReader(keyIndexStream);
            _docIx = new DocIndexReader(docIndexStream);

            CollectionId = collectionId;
        }

        public static Stream CreateReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
        }

        public (long offset, int length) GetDocumentAddress(long docId)
        {
            return _docIx.Get(docId);
        }

        public (long keyId, long valId)[] GetDocumentMap(long offset, int length)
        {
            return _docs.Get(offset, length);
        }

        public (long offset, int len, byte dataType) GetAddressOfKey(long id)
        {
            return _keyIx.Get(id);
        }

        public (long offset, int len, byte dataType) GetAddressOfValue(long id)
        {
            return _valIx.Get(id);
        }

        public object GetKey(long offset, int len, byte dataType)
        {
            return _keys.Get(offset, len, dataType);
        }

        public object GetValue(long offset, int len, byte dataType)
        {
            return _vals.Get(offset, len, dataType);
        }

        public IEnumerable<ISerializableVector> GetValueConvertedToVectors<T>(long offset, int len, byte dataType, Func<T, IEnumerable<ISerializableVector>> tokenizer)
        {
            return _vals.GetValueConvertedToVectors(offset, len, dataType, tokenizer);
        }

        public int DocumentCount()
        {
            return _docIx.Count;
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
