using System.Runtime.InteropServices;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;

namespace Resin.TextAnalysis
{
    public static class VectorOperations
    {
        public static byte[] GetBytes(this Vector<float> vector)
        {
            var stream = new MemoryStream();
            var storage = (SparseVectorStorage<float>)vector.Storage;

            foreach (var index in storage.Indices)
            {
                if (index > 0)
                    stream.Write(BitConverter.GetBytes(index));
                else
                    break;
            }

            foreach (var value in storage.Values)
            {
                if (value > 0)
                    stream.Write(BitConverter.GetBytes(value));
                else
                    break;
            }

            return stream.ToArray();
        }

        public static Vector<float> ToVector(this byte[] buffer, int componentCount, int numOfDimensions)
        {
            if (buffer.Length - (componentCount * 4) != buffer.Length / (2 * 4))
            {
                throw new ArgumentOutOfRangeException(nameof(componentCount));
            }
            var len = componentCount * sizeof(float);
            var indices = MemoryMarshal.Cast<byte, int>(buffer.AsSpan(0, len)).ToArray();
            var values = MemoryMarshal.Cast<byte, float>(buffer.AsSpan(len, len)).ToArray();
            int ii = 0;
            return CreateVector.SparseOfIndexed(numOfDimensions, indices.Select(index => (index, values[ii++])));
        }

        public static double CosAngle(Vector<float> first, Vector<float> second)
        {
            var dotProduct = first.DotProduct(second);
            var dotSelf1 = first.Norm(2);
            var dotSelf2 = second.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }
    }
}
