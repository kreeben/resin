namespace Resin.KeyValue
{
    public class PageWriter<TKey> where TKey : struct, IEquatable<TKey>, IComparable<TKey>
    {
        private ByteArrayWriter _writer;

        public PageWriter(WriteSession writeTransaction)
        {
            _writer = new ByteArrayWriter(writeTransaction);
        }

        public Stream KeyStream => _writer.KeyStream;

        // True when current page has no remaining capacity for new keys
        public bool IsPageFull => _writer.IsPageFull;

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

        public void PutOrAppend(TKey key, ReadOnlySpan<byte> value)
        {
            _writer.PutOrAppend(key.GetHashCode(), value);
        }

        public void Serialize()
        {
            _writer.Serialize();
        }
    }
}