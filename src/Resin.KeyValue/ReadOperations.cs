using System.Runtime.InteropServices;

namespace Resin.KeyValue
{
    public static class ReadOperations
    {
        public static TKey[] ReadSortedSetOfAllKeysInColumn<TKey>(Stream stream) where TKey : struct, IEquatable<TKey>, IComparable<TKey>
        {
            stream.Position = 0;
            var kbuf = new byte[stream.Length];
            stream.ReadExactly(kbuf);
            var keys = MemoryMarshal.Cast<byte, long>(kbuf);
            int indexOfFirstEmptySlot = -1;
            for (int i = 0; i < keys.Length; i++)
            {
                if (i > 0 && keys[i] == 0)
                {
                    indexOfFirstEmptySlot = i;
                    break;
                }
            }
            var totalNoOfSlots = keys.Length;

            var keyCount = indexOfFirstEmptySlot == -1 ? totalNoOfSlots : indexOfFirstEmptySlot;
            var typedKeys = MemoryMarshal.Cast<long, TKey>(keys.Slice(0, keyCount));
            typedKeys.Sort();
            return typedKeys.ToArray();
        }

        public static Address GetAddress(Stream stream, int index, long offset)
        {
            var relPos = index * Address.Size;
            var absPos = relPos + offset;
            stream.Position = absPos;
            var buf = new byte[Address.Size];
            stream.ReadExactly(buf);
            return Address.Deserialize(buf);
        }

        public static bool KeyExists<TKey>(this TKey key, TKey[] keys) where TKey : struct, IEquatable<TKey>, IComparable<TKey>
        {
            int index = new Span<TKey>(keys).BinarySearch(key);
            return index > -1;
        }

        public static ReadOnlySpan<byte> ReadValue(Stream stream, Address address)
        {
            stream.Position = address.Offset;
            var valueBuf = new byte[address.Length].AsSpan();
            stream.ReadExactly(valueBuf);
            return valueBuf;
        }
    }
}