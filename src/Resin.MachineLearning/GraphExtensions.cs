using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning
{
    public static class GraphExtensions
    {
        public static bool TryAdd(this VectorNode root, VectorNode node, GraphOptions options)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Token.Vector == null ? 0 : node.Token.Vector.CosAngle(cursor.Token.Vector);

                if (angle.Approximates(options.IdenticalAngle, options.Precision))
                {
                    return false; // Node is too similar to an existing node, do not add
                }
                else if (angle > options.FoldAngle)
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

        public static Hit ClosestMatch(this VectorNode root, Vector<float> vector, GraphOptions options)
        {
            var best = root;
            var cursor = root;
            double highscore = 0;

            while (cursor != null)
            {
                var angle = cursor.Token.Vector == null ? 0 : vector.CosAngle(cursor.Token.Vector);

                if (angle > options.FoldAngle)
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }

                    if (angle >= options.IdenticalAngle || angle.Approximates(options.IdenticalAngle, options.Precision))
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

        public static double CosAngle(this Vector<float> first, Vector<float> second)
        {
            var dotProduct = first.DotProduct(second);
            var dotSelf1 = first.L2Norm();
            var dotSelf2 = second.L2Norm();

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public static bool Approximates(this double left, double right, double precision)
        {
            return Math.Abs(left - right) < precision;
        }
    }
}
