using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Resin.IO;
using System.Threading;

namespace Resin.Sys
{
    public static class Util
    {
        private static object Sync = new object();

        private static Int64 GetTicks()
        {
            //lock (Sync)
            //{
                return DateTime.Now.Ticks;
            //}
        }

        public static IEnumerable<char> ReplaceOrAppend(this string input, int index, char newChar)
        {
            var chars = input.ToCharArray();
            if (index == input.Length) return input + newChar;
            chars[index] = newChar;
            return chars;
        }

        public static string ReplaceOrAppendToString(this string input, int index, char newChar)
        {
            return new string(input.ReplaceOrAppend(index, newChar).ToArray());
        }

        public static long GetChronologicalFileId()
        {
            return GetTicks();
        }

        private static IEnumerable<string> GetIndexFileNames(string directory)
        {
            return Directory.GetFiles(directory, "*.ix");
        }

        public static IEnumerable<string> GetIndexFileNamesInChronologicalOrder(string directory)
        {
            return GetIndexFileNames(directory)
                .Select(f => new {id = long.Parse(new FileInfo(f).Name.Replace(".ix", "")), fileName = f})
                .OrderBy(info => info.id)
                .Select(info => info.fileName);
        }

        public static int GetDocumentCount(string directory)
        {
            return GetIndexFileNamesInChronologicalOrder(directory)
                .Select(IxInfo.Load)
                .Sum(x=>x.DocumentCount);   
        }

        public static int GetDocumentCount(IEnumerable<IxInfo> ixs)
        {
            return ixs.Sum(x => x.DocumentCount);
        }

        /// <summary>
        /// Divides a list into batches.
        /// </summary>
        public static IEnumerable<IEnumerable<T>> IntoBatches<T>(this IEnumerable<T> list, int size)
        {
            if (size < 1)
            {
                yield return list;
            }
            else
            {
                var count = 0;
                var batch = new List<T>();
                foreach (var item in list)
                {
                    batch.Add(item);
                    if (size == ++count)
                    {
                        yield return batch;
                        batch = new List<T>();
                        count = 0;
                    }
                }
                if (batch.Count > 0) yield return batch;
            }
        }

        public static IDictionary<string, object> ToDictionary(dynamic obj)
        {
            var dictionary = new Dictionary<string, object>();
            foreach (var propertyInfo in obj.GetType().GetProperties())
                if (propertyInfo.CanRead && propertyInfo.GetIndexParameters().Length == 0)
                    dictionary[propertyInfo.Name] = propertyInfo.GetValue(obj, null);
            return dictionary;
        }
    }
}