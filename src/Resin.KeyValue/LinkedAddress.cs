using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    // Node stored in the value stream that links multiple values for a key.
    // The address entry for a key points to the head node: Address.Offset=headPosition, Address.Length=Size.
    public struct LinkedAddressNode
    {
        public const long Magic = 0x4C494E4B4C494E4B; // "LINKLINK" packed
        public long Header;            // Magic
        public Address Target;         // points to the actual value bytes
        public long NextOffset;        // 0 when tail, else absolute offset of next LinkedAddressNode

        public static int Size => sizeof(long) + Address.Size + sizeof(long);

        public LinkedAddressNode(Address target, long nextOffset)
        {
            Header = Magic;
            Target = target;
            NextOffset = nextOffset;
        }

        public static LinkedAddressNode Deserialize(Span<byte> buf)
        {
            var longs = MemoryMarshal.Cast<byte, long>(buf);
            var header = longs[0];
            var target = new Address(longs[1], longs[2]);
            var next = longs[3];
            return new LinkedAddressNode(target, next) { Header = header };
        }

        public static void Serialize(Stream stream, in LinkedAddressNode node)
        {
            stream.Write(BitConverter.GetBytes(node.Header));
            stream.Write(BitConverter.GetBytes(node.Target.Offset));
            stream.Write(BitConverter.GetBytes(node.Target.Length));
            stream.Write(BitConverter.GetBytes(node.NextOffset));
        }

        public static void OverwriteNextOffset(Stream stream, long nodeOffset, long nextOffset)
        {
            // NextOffset field starts after Header (8) + Address (16)
            var nextPos = nodeOffset + sizeof(long) + Address.Size;
            stream.Position = nextPos;
            stream.Write(BitConverter.GetBytes(nextOffset));
        }
    }
}