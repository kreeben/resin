using System.Diagnostics;
using System.Runtime.InteropServices;

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

        public static Address Deserialize(Span<byte> buf)
        {
            Span<long> adrSpan = MemoryMarshal.Cast<byte, long>(buf);
            long ofs = adrSpan[0];
            long len = adrSpan[1];
            return new Address(ofs, len);
        }

        public static void Serialize(Stream stream, Address[] addresses)
        {
            foreach (var adr in addresses)
            {
                stream.Write(BitConverter.GetBytes(adr.Offset));
                stream.Write(BitConverter.GetBytes(adr.Length));
            }
        }
    }
}
