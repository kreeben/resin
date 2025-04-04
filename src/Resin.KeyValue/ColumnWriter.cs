using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public class ColumnWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly PageWriter<TKey> _writer;
        private TKey[] _allKeys;

        public ColumnWriter(PageWriter<TKey> writer)
        {
            _writer = writer;
            _allKeys = ReadAllKeysInColumn();
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (KeyExists(key))
            {
                return false;
            }

            var put = _writer.TryPut(key, value);
            if (put && _writer.IsPageFull)
            {
                Serialize();
            }
            return put;
        }

        public void PutOrAppend(TKey key, ReadOnlySpan<byte> value)
        {
            _writer.PutOrAppend(key, value);
            if (_writer.IsPageFull)
            {
                Serialize();
            }
        }

        private bool KeyExists(TKey key)
        {
            var ix = new Span<TKey>(_allKeys);
            ix.Sort();
            int index = ix.BinarySearch(key);
            return index > -1;
        }

        private TKey[] ReadAllKeysInColumn()
        {
            _writer.KeyStream.Position = 0;
            var kbuf = new byte[_writer.KeyStream.Length];
            _writer.KeyStream.ReadExactly(kbuf);
            var keys = MemoryMarshal.Cast<byte, TKey>(kbuf);
            keys.Sort();
            return keys.ToArray();
        }

        public void Serialize()
        {
            _writer.Serialize();
            _allKeys = ReadAllKeysInColumn();
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                Serialize();
            }
        }
    }

    public class MultiColumnWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly IDictionary<ulong, WriteTransaction> _transactions;
        private readonly ulong _collectionId;
        private readonly DirectoryInfo _directoryInfo;

        private WriteTransaction CreateWriteTransaction(ulong keyHash)
        {
            if (!_transactions.TryGetValue(keyHash, out var tx))
            {
                tx = new WriteTransaction(_directoryInfo, _collectionId);
                _transactions.Add(keyHash, tx);
            }
            return tx;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}