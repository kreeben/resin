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
