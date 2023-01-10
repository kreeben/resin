using System;
using System.Collections.Generic;
using System.IO;

namespace Sir.IO
{
    public static class GraphBuilder
    {
        public static VectorNode CreateTree<T>(this IModel<T> model, IIndexReadWriteStrategy indexingStrategy, params T[] data)
        {
            var root = new VectorNode();
            var embedding = new SortedList<int, float>();

            foreach (var item in data)
            {
                foreach (var vector in model.CreateEmbedding(item, true, embedding))
                {
                    indexingStrategy.Put<T>(root, new VectorNode(vector));
                }
            }

            return root;
        }

        public static void AddOrAppendSupervised(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    if (!cursor.Vector.Label.Equals(node.Vector.Label))
                        throw new InvalidOperationException($"IdenticalAngle {model.IdenticalAngle} is too low. Angle was {angle}");

                    AppendDocIds(cursor, node);
                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
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
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void AddOrAppend(
            this VectorNode root, 
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    AppendDocIds(cursor, node);

                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
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
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void AddIfUnique(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
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
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static bool TryAdd(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    return false;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;

                        return true;
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

                        return true;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void Build(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
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
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void AppendDocIds(this VectorNode target, VectorNode source)
        {
            foreach (var d in source.DocIds)
                target.DocIds.Add(d);
        }

        public static void SerializeNode(this VectorNode node, Stream stream)
        {
            long terminator;

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

            stream.Write(BitConverter.GetBytes(node.VectorOffset), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes(node.PostingsOffset), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes((long)node.Vector.ComponentCount), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes(node.Weight), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes(terminator), 0, sizeof(long));
        }

        public static void SerializeNode(this VectorInfo node, Stream stream)
        {
            stream.Write(BitConverter.GetBytes(node.Angle));
            stream.Write(BitConverter.GetBytes(node.VectorOffset));
            stream.Write(BitConverter.GetBytes(node.PostingsOffset));
            stream.Write(BitConverter.GetBytes(node.ComponentCount));
        }

        /// <summary>
        /// Persist tree to disk.
        /// </summary>
        /// <param name="node">Tree to persist.</param>
        /// <param name="indexStream">stream to persist tree into</param>
        /// <param name="vectorStream">stream to persist vectors into</param>
        /// <param name="postingsStream">stream to persist postings into</param>
        /// <returns></returns>
        public static (long offset, long length) SerializeTree(this VectorNode node, Stream indexStream = null, Stream vectorStream = null, Stream postingsStream = null)
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
                if (node.PostingsOffset == -1 && postingsStream != null)
                {
                    SerializePostings(node, postingsStream);
                }

                if (vectorStream != null)
                {
                    node.VectorOffset = SerializableVector.Serialize(node.Vector, vectorStream);
                }

                if (indexStream != null)
                {
                    SerializeNode(node, indexStream);

                    length += VectorNode.BlockSize;
                }

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

        public static (long offset, long length) SerializeTree(this SortedList<double, VectorInfo> tree, Stream indexStream, Stream postingsStream )
        {
            var offset = indexStream.Position;
            var length = 0;

            foreach (var node in tree)
            {
                SerializePostings(node.Value, postingsStream);

                if (indexStream != null)
                {
                    SerializeNode(node.Value, indexStream);

                    length += VectorRecord.BlockSize;
                }
            }

            return (offset, length);
        }

        public static void SerializePostings(VectorNode node, Stream postingsStream)
        {
            if (node.DocIds.Count == 0) throw new ArgumentException("can't be empty", nameof(node.DocIds));

            node.PostingsOffset = postingsStream.Position;

            // serialize item count
            postingsStream.Write(BitConverter.GetBytes((long)node.DocIds.Count));

            // serialize address of next page (unknown at this time)
            postingsStream.Write(BitConverter.GetBytes((long)0));

            foreach (var docId in node.DocIds)
            {
                postingsStream.Write(BitConverter.GetBytes(docId));
            }
        }

        public static void SerializePostings(VectorInfo node, Stream postingsStream)
        {
            if (node.DocIds.Count == 0) throw new ArgumentException("can't be empty", nameof(node.DocIds));

            node.PostingsOffset = postingsStream.Position;

            // serialize item count
            postingsStream.Write(BitConverter.GetBytes((long)node.DocIds.Count));

            // serialize address of next page (unknown at this time)
            postingsStream.Write(BitConverter.GetBytes((long)0));

            foreach (var docId in node.DocIds)
            {
                postingsStream.Write(BitConverter.GetBytes(docId));
            }
        }
    }
}