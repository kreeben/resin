namespace Resin.KeyValue
{
    public class PageWriter<TKey> : IDisposable where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private readonly ByteArrayWriter _writer;

        public PageWriter(WriteSession writeTransaction)
        {
            _writer = new ByteArrayWriter(writeTransaction);
        }

        public Stream KeyStream => _writer.KeyStream;
        public Stream AddressStream => _writer.AddressStream;
        public Stream ValueStream => _writer.ValueStream;

        // True when current page has no remaining capacity for new keys
        public bool IsPageFull => _writer.IsPageFull;

        /// <summary>
        /// TryPut writes into the current in-memory page only.
        /// - Returns false if the key already exists in the current page (checked via ByteArrayWriter.TryPut's binary search).
        /// - Does not scan previous pages; column-wide duplicate detection is handled by ColumnWriter.TryPut.
        /// </summary>
        public bool TryPut(TKey key, ReadOnlySpan<byte> value)
        {
            if (key is double d)
            {
                return _writer.TryPut(d, value);
            }
            else if (key is float f)
            {
                return _writer.TryPut(f, value);
            }
            else if (key is long l)
            {
                return _writer.TryPut(l, value);
            }
            else if (key is int i)
            {
                return _writer.TryPut(i, value);
            }
            else
            {
                return _writer.TryPut(key.GetHashCode(), value);
            }
        }

        /// <summary>
        /// PutOrAppend operates at the page level:
        /// - If the key exists in the current page, the value is appended to the existing record.
        /// - Otherwise, inserts a new record in the current page.
        /// </summary>
        public void PutOrAppend(TKey key, ReadOnlySpan<byte> value)
        {
            _writer.PutOrAppend(key.GetHashCode(), value);
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
            _writer.Dispose();
        }
    }
}