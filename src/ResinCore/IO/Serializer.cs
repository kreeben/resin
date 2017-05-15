using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Resin.IO.Read;
using Resin.Sys;
using log4net;

namespace Resin.IO
{
    public static class Serializer
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Serializer));

        public static readonly Encoding Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        private static readonly Dictionary<bool, byte> EncodedBoolean = new Dictionary<bool, byte> { { true, 1 }, { false, 0 } };

        public static int SizeOfNode()
        {
            return sizeof(char) + 3 * sizeof(byte) + 1 * sizeof(int) + 1 * sizeof(short) + SizeOfBlock();
        }

        public static int SizeOfBlock()
        {
            return 1 * sizeof(long) + 1 * sizeof(int);
        }

        public static int SizeOfPosting()
        {
            return 2 * sizeof(int);
        }

        public static int SizeOfDocHash()
        {
            return sizeof (UInt64) + sizeof (byte);
        }

        public static void Serialize(this IxInfo ix, string fileName)
        {
            using (var fs = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
            {
                var bytes = ix.Serialize();

                fs.Write(bytes, 0, bytes.Length);
            }
        }

        public static void Serialize(this LcrsTrie trie, string fileName)
        {
            using (var stream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                if (trie.LeftChild != null)
                {
                    trie.LeftChild.SerializeDepthFirst(stream, 0, 0);
                }
            }
        }

        private static void SerializeDepthFirst(this LcrsTrie trie, Stream stream, short depth, int count)
        {
            if (count++ > 1000 * 100)
            {
                Log.Info("cut off trie at 1000 * 100");
                return;
            }

            new LcrsNode(trie, depth, trie.Weight, trie.PostingsAddress).Serialize(stream);

            if (trie.LeftChild != null)
            {
                trie.LeftChild.SerializeDepthFirst(stream, (short)(depth + 1), count);
            }

            if (trie.RightSibling != null)
            {
                trie.RightSibling.SerializeDepthFirst(stream, depth, count);
            }
        }

        public static void Serialize(this LcrsNode node, Stream stream)
        {
            var valBytes = BitConverter.GetBytes(node.Value);
            var byte0 = EncodedBoolean[node.HaveSibling];
            var byte1 = EncodedBoolean[node.HaveChild];
            var byte2 = EncodedBoolean[node.EndOfWord];
            var depthBytes = BitConverter.GetBytes(node.Depth);
            var weightBytes = BitConverter.GetBytes(node.Weight);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(valBytes);
                Array.Reverse(depthBytes);
                Array.Reverse(weightBytes);
            }

            stream.Write(valBytes, 0, valBytes.Length);
            stream.WriteByte(byte0);
            stream.WriteByte(byte1);
            stream.WriteByte(byte2);
            stream.Write(depthBytes, 0, depthBytes.Length);
            stream.Write(weightBytes, 0, weightBytes.Length);

            Serialize(node.PostingsAddress, stream);
        }

        public static void Serialize(this BlockInfo? block, Stream stream)
        {
            if (block == null)
            {
                var pos = BitConverter.GetBytes(long.MinValue);
                var len = BitConverter.GetBytes(int.MinValue);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(pos);
                    Array.Reverse(len);
                }

                stream.Write(pos, 0, pos.Length);
                stream.Write(len, 0, len.Length);
            }
            else
            {
                var blockBytes = block.Value.Serialize();

                stream.Write(blockBytes, 0, blockBytes.Length);
            }
        }

        public static byte[] Serialize(this Document document, Compression compression)
        {
            using (var stream = new MemoryStream())
            {
                var idBytes = BitConverter.GetBytes(document.Id);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(idBytes);
                }

                stream.Write(idBytes, 0, idBytes.Length);

                document.Fields.Values.Serialize(compression, stream);

                return stream.ToArray();
            }
        }

        public static byte[] Serialize(this BlockInfo block)
        {
            using (var stream = new MemoryStream())
            {
                byte[] posBytes = BitConverter.GetBytes(block.Position);
                byte[] lenBytes = BitConverter.GetBytes(block.Length);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(posBytes);
                    Array.Reverse(lenBytes);
                }

                stream.Write(posBytes, 0, posBytes.Length);
                stream.Write(lenBytes, 0, lenBytes.Length);

                return stream.ToArray();
            }
        }

        public static byte[] Serialize(this IxInfo ix)
        {
            using (var stream = new MemoryStream())
            {
                byte[] versionBytes = BitConverter.GetBytes(ix.VersionId);
                byte[] docCountBytes = BitConverter.GetBytes(ix.DocumentCount);
                byte[] compressionEnumBytes = BitConverter.GetBytes((int)ix.Compression);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(versionBytes);
                    Array.Reverse(docCountBytes);
                    Array.Reverse(compressionEnumBytes);
                }

                stream.Write(versionBytes, 0, sizeof(long));
                stream.Write(docCountBytes, 0, sizeof(int));
                stream.Write(compressionEnumBytes, 0, sizeof(int));

                return stream.ToArray();
            }
        }

        public static IxInfo DeserializeIxInfo(Stream stream)
        {
            var versionBytes = new byte[sizeof(long)];

            stream.Read(versionBytes, 0, sizeof(long));

            var docCountBytes = new byte[sizeof(int)];

            stream.Read(docCountBytes, 0, sizeof(int));

            var compression = new byte[sizeof(int)];

            stream.Read(compression, 0, sizeof(int));

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(versionBytes);
                Array.Reverse(docCountBytes);
                Array.Reverse(compression);
            }

            return new IxInfo
            {
                VersionId= BitConverter.ToInt64(versionBytes, 0),
                DocumentCount = BitConverter.ToInt32(docCountBytes, 0),
                Compression = (Compression)BitConverter.ToInt32(compression, 0),
            };
        }

        public static byte[] Serialize(this IEnumerable<KeyValuePair<string, int>> entries)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var entry in entries)
                {
                    byte[] keyBytes = Encoding.GetBytes(entry.Key);
                    byte[] lengthBytes = BitConverter.GetBytes((short)keyBytes.Length);
                    byte[] intBytes = BitConverter.GetBytes(entry.Value);

                    if (!BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(lengthBytes);
                        Array.Reverse(keyBytes);
                        Array.Reverse(intBytes);
                    }

                    stream.Write(lengthBytes, 0, sizeof(short));
                    stream.Write(keyBytes, 0, keyBytes.Length);
                    stream.Write(intBytes, 0, sizeof(int));
                }
                return stream.ToArray();
            }
        }

        public static byte[] Serialize(this IEnumerable<KeyValuePair<int, string>> entries)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var entry in entries)
                {
                    byte[] valBytes = Encoding.GetBytes(entry.Value);
                    byte[] valLenBytes = BitConverter.GetBytes((short)valBytes.Length);
                    byte[] keyBytes = BitConverter.GetBytes(entry.Key);

                    if (!BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(valLenBytes);
                        Array.Reverse(keyBytes);
                        Array.Reverse(valBytes);
                    }

                    stream.Write(keyBytes, 0, sizeof(int));
                    stream.Write(valLenBytes, 0, sizeof(short));
                    stream.Write(valBytes, 0, valBytes.Length);
                }
                return stream.ToArray();
            }
        }

        public static void Serialize(this IEnumerable<Field> fields, Compression compression, Stream stream)
        {
            foreach (var field in fields)
            {
                byte[] keyBytes = Encoding.GetBytes(field.Key);
                byte[] keyLengthBytes = BitConverter.GetBytes((short)keyBytes.Length);
                byte[] valBytes;
                string toStore = field.Store ? field.Value : string.Empty;

                if (compression == Compression.GZip)
                {
                    valBytes = Deflator.Compress(Encoding.GetBytes(toStore));
                }
                else if (compression == Compression.Lz)
                {
                    valBytes = QuickLZ.compress(Encoding.GetBytes(toStore), 1);
                }
                else
                {
                    valBytes = Encoding.GetBytes(toStore);
                }

                byte[] valLengthBytes = BitConverter.GetBytes(valBytes.Length);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(keyLengthBytes);
                    Array.Reverse(keyBytes);
                    Array.Reverse(valBytes);
                    Array.Reverse(valLengthBytes);
                }

                stream.Write(keyLengthBytes, 0, sizeof(short));
                stream.Write(keyBytes, 0, keyBytes.Length);
                stream.Write(valLengthBytes, 0, sizeof(int));
                stream.Write(valBytes, 0, valBytes.Length);
            }
        }

        public static byte[] Serialize(this IEnumerable<DocumentPosting> postings)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var posting in postings)
                {
                    byte[] idBytes = BitConverter.GetBytes(posting.DocumentId);
                    byte[] countBytes = BitConverter.GetBytes(posting.Count);

                    if (!BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(idBytes);
                        Array.Reverse(countBytes);
                    }

                    stream.Write(idBytes, 0, sizeof(int));
                    stream.Write(countBytes, 0, sizeof(int));
                }
                return stream.ToArray();
            }
        }

        public static byte[] Serialize(this IEnumerable<int> entries)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var entry in entries)
                {
                    byte[] valBytes = BitConverter.GetBytes(entry);

                    if (!BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(valBytes);
                    }

                    stream.Write(valBytes, 0, sizeof(int));
                }
                return stream.ToArray();
            }
        }

        public static void Serialize(this IEnumerable<DocHash> docHashes, string fileName)
        {
            using (var stream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                foreach (var docHash in docHashes)
                {
                    docHash.Serialize(stream);
                }
            }
        }

        public static void Serialize(this DocHash docHash, Stream stream)
        {
            byte[] hashBytes = BitConverter.GetBytes(docHash.Hash);
            byte isObsoleteByte = EncodedBoolean[docHash.IsObsolete];

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(hashBytes);
            }

            stream.WriteByte(isObsoleteByte);
            stream.Write(hashBytes, 0, sizeof(UInt64));
        }

        public static byte[] DeSerializeFile(string fileName)
        {
            using (var stream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                return ReadToEnd(stream);
            }
        }

        //http://stackoverflow.com/questions/221925/creating-a-byte-array-from-a-stream
        public static byte[] ReadToEnd(Stream input)
        {
            var buffer = new byte[16 * 1024];
            using (var ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }

        public static LcrsNode DeserializeNode(Stream stream)
        {
            if (!stream.CanRead) return LcrsNode.MinValue;

            var valBytes = new byte[sizeof(char)];
            var depthBytes = new byte[sizeof(short)];
            var weightBytes = new byte[sizeof(int)];

            stream.Read(valBytes, 0, sizeof (char));
            int byte0 = stream.ReadByte();
            int byte1 = stream.ReadByte();
            int byte2 = stream.ReadByte();
            stream.Read(depthBytes, 0, depthBytes.Length);
            stream.Read(weightBytes, 0, weightBytes.Length);
            BlockInfo? block = DeserializeBlock(stream);
            
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(valBytes);
                Array.Reverse(depthBytes);
                Array.Reverse(weightBytes);
            }

            return new LcrsNode(
                BitConverter.ToChar(valBytes, 0),
                byte0 == 1,
                byte1 == 1,
                byte2 == 1,
                BitConverter.ToInt16(depthBytes, 0),
                BitConverter.ToInt32(weightBytes, 0),
                block);
        }

        public static BlockInfo? DeserializeBlock(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DeserializeBlock(stream);
            }
        }

        public static Document DeserializeDocument(byte[] data, Compression compression)
        {
            var idBytes = new byte[sizeof(int)];
            Array.Copy(data, 0, idBytes, 0, sizeof(int));

            var dicBytes = new byte[data.Length - sizeof(int)];
            Array.Copy(data, sizeof(int), dicBytes, 0, dicBytes.Length);
            
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(idBytes);
                Array.Reverse(dicBytes);
            }

            var id = BitConverter.ToInt32(idBytes, 0);
            var doc = DeserializeFields(dicBytes, compression).ToList();

            return new Document(doc) { Id = id };
        }

        public static BlockInfo DeserializeBlock(Stream stream)
        {
            var posBytes = new byte[sizeof(long)];
            var lenBytes = new byte[sizeof(int)];

            stream.Read(posBytes, 0, posBytes.Length);
            stream.Read(lenBytes, 0, lenBytes.Length);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(posBytes);
                Array.Reverse(lenBytes);
            }

            return new BlockInfo(BitConverter.ToInt64(posBytes, 0), BitConverter.ToInt32(lenBytes, 0));
        }

        public static IEnumerable<int> DeserializeIntList(byte[] data)
        {
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(data);
            }

            int pos = 0;

            while (pos < data.Length)
            {
                var val = BitConverter.ToInt32(data, pos);

                yield return val;

                pos = pos + sizeof(int);
            }
        }

        public static DocHash DeserializeDocHash(Stream stream)
        {
            var isObsoleteByte = stream.ReadByte();

            if (isObsoleteByte == 1) return null;

            var hashBytes = new byte[sizeof(UInt64)];

            stream.Read(hashBytes, 0, sizeof(UInt64));

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(hashBytes);
            }

            return new DocHash(BitConverter.ToUInt64(hashBytes, 0), isObsoleteByte == 1);
        }

        public static IList<DocHash> DeserializeDocHashes(string fileName)
        {
            using (var stream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                return DeserializeDocHashes(stream).ToList();
            }
        }

        public static IEnumerable<DocHash> DeserializeDocHashes(Stream stream)
        {
            while (true)
            {
                var hash = DeserializeDocHash(stream);

                if (hash == null) break;

                yield return hash;
            }
        }

        public static IEnumerable<DocumentPosting> DeserializePostings(byte[] data)
        {
            var chunk = new byte[SizeOfPosting()];
            int pos = 0;

            while (pos<data.Length)
            {
                Array.Copy(data, pos, chunk, 0, chunk.Length);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(chunk);//ain't gonna work. TODO: reverse individual byte streams, not entire chunk (this is a bug).
                }

                yield return new DocumentPosting(
                    BitConverter.ToInt32(chunk, 0),
                    BitConverter.ToInt32(chunk, sizeof(int))
                    );

                pos = pos + chunk.Length; 
            }
        }

        public static IEnumerable<Field> DeserializeFields(byte[] data, Compression compression)
        {
            using (var stream = new MemoryStream(data))
            {
                return DeserializeFields(stream, compression).ToList();
            }
        }

        public static IEnumerable<Field> DeserializeFields(Stream stream, Compression compression)
        {
            while (true)
            {
                var keyLengthBytes = new byte[sizeof(short)];

                var read = stream.Read(keyLengthBytes, 0, sizeof(short));

                if (read == 0) break;

                short keyLength = BitConverter.ToInt16(keyLengthBytes, 0);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(keyLengthBytes);
                }

                byte[] keyBytes = new byte[keyLength];

                stream.Read(keyBytes, 0, keyLength);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(keyBytes);
                }

                string key = Encoding.GetString(keyBytes);

                var valLengthBytes = new byte[sizeof(int)];

                stream.Read(valLengthBytes, 0, sizeof(int));

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(valLengthBytes);
                }

                int valLength = BitConverter.ToInt32(valLengthBytes, 0);

                byte[] valBytes = new byte[valLength];

                stream.Read(valBytes, 0, valLength);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(valBytes);
                }


                string value;

                if (compression == Compression.GZip)
                {
                    value = Encoding.GetString(Deflator.Deflate(valBytes));
                }
                else if (compression == Compression.Lz)
                {
                    value = Encoding.GetString(QuickLZ.decompress(valBytes));
                }
                else
                {
                    value = Encoding.GetString(valBytes);
                }


                yield return new Field(key, value);
            }
        }

        public static IEnumerable<KeyValuePair<int, string>> DeserializeIntStringDic(Stream stream)
        {
            while (true)
            {
                var keyBytes = new byte[sizeof(int)];

                stream.Read(keyBytes, 0, sizeof(int));

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(keyBytes);
                }

                int key = BitConverter.ToInt32(keyBytes, 0);

                var valLenBytes = new byte[sizeof(short)];

                var read = stream.Read(valLenBytes, 0, sizeof(short));

                if (read == 0) break;

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(valLenBytes);
                }

                short valLen = BitConverter.ToInt16(valLenBytes, 0);

                byte[] valBytes = new byte[valLen];

                stream.Read(valBytes, 0, valLen);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(valBytes);
                }

                string value = Encoding.GetString(valBytes);

                yield return new KeyValuePair<int, string>(key, value);
            }
        }

        public static IEnumerable<KeyValuePair<string, int>> DeserializeStringIntDic(Stream stream)
        {
            while (true)
            {
                var lengthBytes = new byte[sizeof(short)];

                var read = stream.Read(lengthBytes, 0, sizeof (short));

                if (read == 0) break;

                short keyLength = BitConverter.ToInt16(lengthBytes, 0);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(lengthBytes);
                }

                byte[] keyBytes = new byte[keyLength];

                stream.Read(keyBytes, 0, keyLength);

                string key = Encoding.GetString(keyBytes);

                var intBytes = new byte[sizeof(int)];

                stream.Read(intBytes, 0, sizeof (int));

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(intBytes);
                }

                int value = BitConverter.ToInt32(intBytes, 0);
                
                yield return new KeyValuePair<string, int>(key, value);
            }
        }

        public static LcrsTrie DeserializeTrie(string directory, long indexVersionId, string field)
        {
            var searchPattern = string.Format("{0}-{1}-*", indexVersionId, field.ToHash());

            return DeserializeTrie(directory, searchPattern);
        }

        public static LcrsTrie DeserializeTrie(string directory, string searchPattern)
        {
            var root = new LcrsTrie();
            LcrsTrie next = null;

            foreach (var fileName in Directory.GetFiles(directory, searchPattern).OrderBy(f => f))
            {
                using (var reader = new MappedTrieReader(fileName))
                {
                    var trie = reader.ReadWholeFile();

                    if (next == null)
                    {
                        root.LeftChild = trie;
                    }
                    else
                    {
                        next.RightSibling = trie;
                    }
                    next = trie;
                }
            }

            return root;
        }
    }
}