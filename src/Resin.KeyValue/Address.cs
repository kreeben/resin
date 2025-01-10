using System.Diagnostics;

namespace Resin.KeyValue
{
    [DebuggerDisplay("{Offset}, {Length}")]
    public struct Address : IEquatable<Address>, IComparable<Address>
    {
        public long Offset; public long Length;
        public static int Size { get { return sizeof(long) * 2; } }
        private static Address EMPTY = new Address(long.MaxValue, long.MaxValue);

        public Address(long offset, long length)
        {
            Offset = offset; Length = length;
        }

        public int CompareTo(Address other)
        {
            return Offset.CompareTo(other.Offset);
        }

        public int CompareTo(Object other)
        {
            return Offset.CompareTo(((Address)other).Offset);
        }

        public bool Equals(Address other)
        {
            return Offset.Equals(other.Offset);
        }

        public static Address Empty()
        {
            return EMPTY;
        }
    }
}
