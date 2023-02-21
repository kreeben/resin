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

        public static bool Approximates(this double left, double right, double precision)
        {
            return Math.Abs(left - right) < precision;
        }

        public static bool Approximates(this float left, float right, double precision)
        {
            return Math.Abs(left - right) < precision;
        }
    }
}
