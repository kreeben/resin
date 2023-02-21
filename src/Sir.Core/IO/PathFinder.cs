using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Sir.IO
{
    public static class PathFinder
    {
        public static Hit ClosestMatch(VectorNode root, ISerializableVector vector, IModel model)
        {
            var best = root;
            var cursor = root;
            double highscore = 0;
            
            while (cursor != null)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(vector, cursor.Vector);

                if (angle > model.FoldAngle)
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }

                    if (angle >= model.IdenticalAngle)
                    {
                        break;
                    }

                    cursor = cursor.Left;
                }
                else
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }

                    cursor = cursor.Right;
                }
            }

            return new Hit(best, highscore);
        }

        public static IEnumerable<VectorNode> All(VectorNode root)
        {
            var node = root.Vector == null ? root.Right : root;
            var stack = new Stack<VectorNode>();

            while (node != null)
            {
                yield return node;

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null)
                {
                    if (stack.Count > 0)
                        node = stack.Pop();
                }
            }
        }

        public static IEnumerable<VectorNode> RightList(VectorNode root)
        {
            var node = root.Right;

            while (node != null)
            {
                yield return node;

                node = node.Right;
            }
        }

        public static IEnumerable<VectorNode> LeftList(VectorNode root)
        {
            var node = root.Left;

            while (node != null)
            {
                yield return node;

                node = node.Left;
            }
        }

        public static float[][] AsOneHotMatrix(VectorNode root)
        {
            var node = root.Vector == null ? root.Right : root;
            var stack = new Stack<VectorNode>();
            var matrix = new float[root.Weight][];
            var index = 0;

            while (node != null)
            {
                var vector = new float[root.Weight];

                vector[index] = 1;
                matrix[index] = vector;

                index++;

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null)
                {
                    if (stack.Count > 0)
                        node = stack.Pop();
                }
            }

            return matrix;
        }

        public static string Visualize(VectorNode root)
        {
            StringBuilder output = new StringBuilder();
            var depth = 0;

            Visualize(root, depth, output);

            var node = root.Right;
            var stack = new Stack<(VectorNode node, int depth)>();

            while (node != null)
            {
                Visualize(node, depth, output);

                if (node.Right != null)
                {
                    stack.Push((node.Right, depth));
                }

                node = node.Left;

                if (node == null)
                {
                    if (stack.Count > 0)
                    {
                        var n = stack.Pop();
                        node = n.node;
                        depth = n.depth;
                    }
                }
                else
                {
                    depth++;
                }
            }

            return output.ToString();
        }

        public static string Visualize2(VectorNode root)
        {
            StringBuilder output = new StringBuilder();

            foreach (var rightNode in root.RightNodes)
            {
                output.AppendLine($"{rightNode} w:{rightNode.LeftNodes.Count}");

                foreach (var leftNode in rightNode.LeftNodes)
                {
                    output.AppendLine($"\t{string.Join(" ", leftNode.Value.labels)} a:{leftNode.Key} w:{leftNode.Value.documents.Count}");
                }
            }

            return output.ToString();
        }

        private static void Visualize(VectorNode node, int depth, StringBuilder output)
        {
            if (node == null) return;

            output.Append('\t', depth);
            output.AppendFormat($"{node} w:{node.Weight} ");

            if (node.IsRoot)
                output.AppendFormat($"{Size(node)}");

            output.AppendLine();
        }

        public static (int depth, int width) Size(VectorNode root)
        {
            var node = root.Right;
            var width = 0;
            var depth = 0;

            while (node != null)
            {
                var d = Depth(node);

                if (d > depth)
                {
                    depth = d;
                }

                width++;

                node = node.Right;
            }

            return (depth, width);
        }

        private static int Depth(VectorNode node)
        {
            var count = 0;

            node = node.Left;

            while (node != null)
            {
                count++;
                node = node.Left;
            }

            return count;
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
            IDistanceCalculator model)
        {
            var vector = SerializableVector.Deserialize(vecOffset, (int)componentCount, model.NumOfDimensions, vectorStream);
            var node = new VectorNode(vector, postingsOffset, vecOffset, terminator, weight);

            return node;
        }

        public static VectorNode DeserializeTree(Stream indexStream, Stream vectorStream, IModel model, long pageSize)
        {
            VectorNode root = new VectorNode();
            VectorNode cursor = root;
            var stack = new Stack<VectorNode>();
            var buf = new byte[VectorNode.BlockSize];
            long read = 0;

            while (read < pageSize)
            {
                read += indexStream.Read(buf);

                if (read == 0)
                    break;

                var node = DeserializeNode(buf, vectorStream, model);

                if (node.Terminator == 0) // there is both a left and a right child
                {
                    cursor.Left = node;
                    stack.Push(cursor);
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
                    if (stack.Count > 0)
                    {
                        stack.Pop().Right = node;
                    }
                }

                cursor = node;
            }

            return root;
        }
    }
}
