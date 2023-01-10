using MathNet.Numerics.LinearAlgebra;
using System;

namespace Sir
{
    public static class DoubleExtensions
    {
        private const double _precision = 0.01;

        public static bool Approximates(this double left, double right)
        {
            return Math.Abs(left - right) < _precision;
        }

        public static bool Approximates(this float left, float right)
        {
            return Math.Abs(left - right) < _precision;
        }

        public static double CosAngle(this Vector<float> vec1, Vector<float> vec2)
        {
            var dotProduct = vec1.DotProduct(vec2);
            var dotSelf1 = vec1.Norm(2);
            var dotSelf2 = vec2.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }
    }
}
