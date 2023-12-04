using System;

namespace Sir
{
    public static class HashOperations
    {
        public static ulong ToHash(this string text)
        {
            return ToHash((IComparable)text);
        }

        public static ulong ToHash(this object text)
        {
            return CalculateKnuthHash(text.ToString());
        }

        private static ulong CalculateKnuthHash(string read)
        {
            UInt64 hashedValue = 3074457345618258791ul;
            for (int i = 0; i < read.Length; i++)
            {
                hashedValue += read[i];
                hashedValue *= 3074457345618258799ul;
            }
            return hashedValue;
        }

        public static long MapToLong(this ulong ulongValue)
        {
            return unchecked((long)ulongValue + long.MinValue);
        }
    }
}