namespace Resin.TextAnalysis
{
    public static class HashOperations
    {
        public static ulong ToHash(this string text)
        {
            return CalculateKnuthHash(text);
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
    }
}
