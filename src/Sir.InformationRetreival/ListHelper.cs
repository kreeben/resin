using System;
using System.Collections.Generic;

namespace Sir
{
    public static class ListHelper
    {
        /// <summary>
        /// https://stackoverflow.com/a/44505349/46645
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size)
        {
            if (size <= 0)
                throw new ArgumentOutOfRangeException("size", "Must be greater than zero.");

            using (var enumerator = source.GetEnumerator())
            while (enumerator.MoveNext())
            {
                int i = 0;
                // Batch is a local function closing over `i` and `enumerator` that
                // executes the inner batch enumeration
                IEnumerable<T> Batch()
                {
                    do yield return enumerator.Current;
                    while (++i < size && enumerator.MoveNext());
                }

                yield return Batch();
                while (++i < size && enumerator.MoveNext()) ; // discard skipped items
            }
        }
    }
}
