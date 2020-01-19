using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.VectorSpace
{
    public static class GraphBuilder
    {
        public static bool TryMerge(
            VectorNode root, 
            VectorNode node,
            IDistanceCalculator model,
            double foldAngle,
            double identicalAngle,
            out VectorNode parent)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    parent = cursor;
                    return true;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        parent = cursor;
                        return false;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;
                        parent = cursor;
                        return false;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static long GetOrIncrementId(
            VectorNode root, 
            VectorNode node,
            IDistanceCalculator model, 
            double foldAngle, 
            double identicalAngle,
            Func<long> identity)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    return cursor.PostingsOffset;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        node.PostingsOffset = identity();
                        cursor.Left = node;
                        return node.PostingsOffset;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        node.PostingsOffset = identity();
                        cursor.Right = node;
                        return node.PostingsOffset;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static bool MergeOrAdd(
            VectorNode root, 
            VectorNode node,
            IDistanceCalculator model, 
            double foldAngle, 
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    AddDocId(cursor, node);

                    return true;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        return false;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;
                        return false;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static bool MergeOrAddConcurrent(
            VectorNode root,
            VectorNode node,
            IDistanceCalculator model,
            double foldAngle,
            double identicalAngle)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= identicalAngle)
                {
                    lock (cursor.Sync)
                    {
                        AddDocId(cursor, node);
                    }

                    return true;
                }
                else if (angle > foldAngle)
                {
                    if (cursor.Left == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Left == null)
                            {
                                cursor.Left = node;
                                return false;
                            }
                            else
                            {
                                cursor = cursor.Left;
                            }
                        }
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Right == null)
                            {
                                cursor.Right = node;
                                return false;
                            }
                            else
                            {
                                cursor = cursor.Right;
                            }
                        }
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void MergePostings(VectorNode target, VectorNode source)
        {
            if (source.PostingsOffsets != null)
                ((List<long>)target.PostingsOffsets).AddRange(source.PostingsOffsets);
        }

        public static void AddDocId(VectorNode target, long docId)
        {
            target.DocIds.Add(docId);
        }

        public static void AddDocId(VectorNode target, VectorNode node)
        {
            foreach (var docId in node.DocIds)
            {
                target.DocIds.Add(docId);
            }
        }

        public static void SerializeNode(VectorNode node, Stream stream)
        {
            long terminator = 1;

            if (node.Left == null && node.Right == null) // there are no children
            {
                terminator = 3;
            }
            else if (node.Left == null) // there is a right but no left
            {
                terminator = 2;
            }
            else if (node.Right == null) // there is a left but no right
            {
                terminator = 1;
            }
            else // there is a left and a right
            {
                terminator = 0;
            }

            Span<long> span = stackalloc long[5];

            span[0] = node.VectorOffset;
            span[1] = node.PostingsOffset;
            span[2] = node.Vector.ComponentCount;
            span[3] = node.Weight;
            span[4] = terminator;

            stream.Write(MemoryMarshal.Cast<long, byte>(span));
        }

        public static (long offset, long length) SerializeTree(
            VectorNode node, Stream indexStream, Stream vectorStream, Stream postingsStream)
        {
            var stack = new Stack<VectorNode>();
            var offset = indexStream.Position;
            var length = 0;

            if (node.ComponentCount == 0)
            {
                node = node.Right;
            }

            while (node != null)
            {
                if (node.PostingsOffset == -1)
                    SerializePostings(node, postingsStream);
                else
                    throw new InvalidComObjectException();

                node.VectorOffset = VectorOperations.SerializeVector(node.Vector, vectorStream);

                SerializeNode(node, indexStream);

                length += VectorNode.BlockSize;

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null && stack.Count > 0)
                {
                    node = stack.Pop();
                }
            }

            return (offset, length);
        }

        public static void SerializePostings(VectorNode node, Stream postingsStream)
        {
            node.PostingsOffset = postingsStream.Position;

            SerializeHeaderAndPayload(node.DocIds, node.DocIds.Count, postingsStream);
        }

        public static void SerializeHeaderAndPayload(IEnumerable<long> items, int itemCount, Stream stream)
        {
            var payload = new long[itemCount + 1];

            payload[0] = itemCount;

            var index = 1;

            foreach (var item in items)
            {
                payload[index++] = item;
            }

            stream.Write(MemoryMarshal.Cast<long, byte>(payload));
        }

        public static VectorNode DeserializeNode(byte[] nodeBuffer, Stream vectorStream, IModel model)
        {
            // Deserialize node
            var vecOffset = BitConverter.ToInt64(nodeBuffer, 0);
            var postingsOffset = BitConverter.ToInt64(nodeBuffer, sizeof(long));
            var vectorCount = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long));
            var weight = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long) + sizeof(long));
            var terminator = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long));

            return DeserializeNode(vecOffset, postingsOffset, vectorCount, weight, terminator, vectorStream, model);
        }

        public static VectorNode DeserializeNode(
            long vecOffset,
            long postingsOffset,
            long componentCount,
            long weight,
            long terminator,
            Stream vectorStream,
            IVectorSpaceConfig model)
        {
            var vector = VectorOperations.DeserializeVector(vecOffset, (int)componentCount, model.VectorWidth, vectorStream);
            var node = new VectorNode(postingsOffset, vecOffset, terminator, weight, vector);

            return node;
        }

        public static void DeserializeUnorderedFile(
            Stream indexStream,
            Stream vectorStream,
            VectorNode root,
            float identicalAngle, 
            float foldAngle,
            IModel model)
        {
            var buf = new byte[VectorNode.BlockSize];
            int read = indexStream.Read(buf);

            while (read == VectorNode.BlockSize)
            {
                var node = DeserializeNode(buf, vectorStream, model);
                VectorNode parent;

                if (TryMerge(root, node, model, model.FoldAngle, model.IdenticalAngle, out parent))
                {
                    MergePostings(parent, node);
                }

                read = indexStream.Read(buf);
            }
        }

        public static void DeserializeTree(
            Stream indexStream,
            Stream vectorStream,
            long indexLength,
            VectorNode root,
            (float identicalAngle, float foldAngle) similarity,
            IModel model)
        {
            int read = 0;
            var buf = new byte[VectorNode.BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, model);
                VectorNode parent;

                if (TryMerge(root, node, model, model.FoldAngle, model.IdenticalAngle, out parent))
                {
                    MergePostings(parent, node);
                }

                read += VectorNode.BlockSize;
            }
        }

        public static VectorNode DeserializeTree(
            Stream indexStream, Stream vectorStream, long indexLength, IModel model)
        {
            VectorNode root = new VectorNode();
            VectorNode cursor = root;
            var tail = new Stack<VectorNode>();
            int read = 0;
            var buf = new byte[VectorNode.BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, model);

                if (node.Terminator == 0) // there is both a left and a right child
                {
                    cursor.Left = node;
                    tail.Push(cursor);
                }
                else if (node.Terminator == 1) // there is a left but no right child
                {
                    cursor.Left = node;
                }
                else if (node.Terminator == 2) // there is a right but no left child
                {
                    cursor.Right = node;
                }
                else // there are no children
                {
                    if (tail.Count > 0)
                    {
                        tail.Pop().Right = node;
                    }
                }

                cursor = node;
                read += VectorNode.BlockSize;
            }

            return root;
        }
    }
}
