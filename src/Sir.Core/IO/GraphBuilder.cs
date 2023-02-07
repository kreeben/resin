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

        public static void AddOrAppendToSortedList(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            if (root.RightNodes == null)
            {
                node.LeftNodes = new SortedList<double, HashSet<long>> { { 1, new HashSet<long> { node.DocId } } };
                root.RightNodes = new List<VectorNode> { node };
            }
            else
            {
                var matched = false;

                foreach (var rightNode in root.RightNodes)
                {
                    var angle = model.CosAngle(node.Vector, rightNode.Vector);

                    if (angle > 0)
                    {
                        matched = true;

                        HashSet<long> documents;

                        if (rightNode.LeftNodes.TryGetValue(angle, out documents))
                        {
                            documents.Add(node.DocId);
                        }
                        else
                        {
                            rightNode.LeftNodes.Add(angle, new HashSet<long> { node.DocId });
                        }
                    }
                }

                if (!matched)
                {
                    node.LeftNodes = new SortedList<double, HashSet<long>> { { 1, new HashSet<long> { node.DocId } } };
                    root.RightNodes.Add(node);
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
    }
}