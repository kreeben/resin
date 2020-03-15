using System;

namespace Sir.VectorSpace
{
    public static class DoubleExtensions
    {
        private const double _precision = 0.01;

        public static bool Approximates(this double left, double right)
        {
            return Math.Abs(left - right) < _precision;
        }
    }
}
