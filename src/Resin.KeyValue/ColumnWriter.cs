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
}