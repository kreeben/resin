namespace Resin.KeyValue
{
    public class ColumnWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly PageWriter<TKey> _writer;
        private TKey[] _allKeys;

        public ColumnWriter(PageWriter<TKey> writer)
        {
            _writer = writer;
            _allKeys = ReadUtil.ReadSortedSetOfAllKeysInColumn<TKey>(writer.KeyStream);
        }

        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (key.KeyExists(_allKeys))
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

        public void Serialize()
        {
            _writer.Serialize();
            _allKeys = ReadUtil.ReadSortedSetOfAllKeysInColumn<TKey>(_writer.KeyStream);
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