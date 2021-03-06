﻿using System.Collections.Generic;
using System.Text;

namespace Sir.VectorSpace
{
    public static class PathFinder
    {
        public static Hit ClosestMatch(VectorNode root, IVector vector, IModel model)
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
    }
}
