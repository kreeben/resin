namespace Resin.KeyValue
{
    public class ColumnWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly PageWriter<TKey> _writer;
        private TKey[] _allKeys;

        public ColumnWriter(PageWriter<TKey> writer)
        {
            _writer = writer;
            // Column-wide snapshot: load all keys for duplicate detection across previous pages.
            _allKeys = ReadOperations.ReadSortedSetOfAllKeysInColumn<TKey>(writer.KeyStream);
        }

        /// <summary>
        /// TryPut writes the key/value only if the key does not already exist in the column snapshot.
        /// - Returns false if the key exists in the cached sorted set of all keys (_allKeys), which represents the whole column.
        /// - Otherwise delegates to PageWriter.TryPut for page-level insertion.
        /// - If the current page becomes full, triggers serialization of the page.
        /// </summary>
        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            // Scan the whole column snapshot (cached in _allKeys) to detect duplicates across previous pages.
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

        /// <summary>
        /// PutOrAppend appends when the key exists in the current page; otherwise inserts a new key in the current page.
        /// - Does not scan the column-wide snapshot for append semantics; operates at the page level.
        /// - If the current page becomes full, triggers serialization of the page.
        /// </summary>
        public void PutOrAppend(TKey key, ReadOnlySpan<byte> value)
        {
            _writer.PutOrAppend(key, value);
            if (_writer.IsPageFull)
            {
                Serialize();
            }
        }

        /// <summary>
        /// Serialize flushes the current page to the backing streams.
        /// </summary>
        public void Serialize()
        {
            _writer.Serialize();
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