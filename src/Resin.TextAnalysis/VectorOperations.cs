using System.Runtime.InteropServices;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;

namespace Resin.TextAnalysis
{
    public static class VectorOperations
    {
        public static byte[] GetBytes<T>(this Vector<T> vector, Func<T, byte[]> serialize) where T : struct, IEquatable<T>, IFormattable
        {
            var stream = new MemoryStream();
            var storage = (SparseVectorStorage<T>)vector.Storage;
            int componentCount = storage.ValueCount;

            stream.Write(BitConverter.GetBytes(componentCount));

            var passedZero = false;
            foreach (var index in storage.Indices)
            {
                if (index == 0 && !passedZero)
                {
                    stream.Write(BitConverter.GetBytes(index));
                    passedZero = true;
                    continue;
                }
                else if (index == 0)
                    break;

                stream.Write(BitConverter.GetBytes(index));
            }

            foreach (var value in storage.Values)
            {
                if (!value.Equals(default(T)))
                    stream.Write(serialize(value));
            }

            return stream.ToArray();
        }

        public static Vector<float> ToVectorFloat(this byte[] bufferWithHeader, int numOfDimensions)
        {
            var componentCount = BitConverter.ToInt32(bufferWithHeader);
            var len = componentCount * sizeof(float);
            var indices = MemoryMarshal.Cast<byte, int>(bufferWithHeader.AsSpan(sizeof(int), len)).ToArray();
            var values = MemoryMarshal.Cast<byte, float>(bufferWithHeader.AsSpan(sizeof(int) + len, len)).ToArray();
            int i = 0;
            return CreateVector.SparseOfIndexed(numOfDimensions, indices.Select(index => (index, values[i++])));
        }

        public static Vector<double> ToVectorDouble(this byte[] bufferWithHeader, int numOfDimensions)
        {
            var componentCount = BitConverter.ToInt32(bufferWithHeader);
            var ixLen = componentCount * sizeof(int);
            var valLen = componentCount * sizeof(double);
            var indices = MemoryMarshal.Cast<byte, int>(bufferWithHeader.AsSpan(sizeof(int), ixLen)).ToArray();
            var values = MemoryMarshal.Cast<byte, double>(bufferWithHeader.AsSpan(sizeof(int) + ixLen, valLen)).ToArray();
            int i = 0;
            return CreateVector.SparseOfIndexed(numOfDimensions, indices.Select(index => (index, values[i++])));
        }

        public static double CosAngle(this Vector<float> first, Vector<float> second)
        {
            var dotProduct = first.DotProduct(second);
            var dotSelf1 = first.Norm(2);
            var dotSelf2 = second.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public static double CosAngle(this Vector<double> first, Vector<double> second)
        {
            var dotProduct = first.DotProduct(second);
            var dotSelf1 = first.Norm(2);
            var dotSelf2 = second.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public static string AsString(this Vector<float> vector)
        {
            var storage = (SparseVectorStorage<float>)vector.Storage;
            var chars = new List<char>();
            for (int i = 0; i < storage.Indices.Length; i++)
            {
                var v = Convert.ToInt32(storage.Values[i]);
                chars.Add((char)v);
                continue;
            }
            return new string(chars.ToArray());
        }

        public static string AsString(this Vector<double> vector)
        {
            var storage = (SparseVectorStorage<double>)vector.Storage;
            var chars = new List<char>();
            for (int i = 0; i < storage.Indices.Length; i++)
            {
                var v = Convert.ToInt64(storage.Values[i]);
                chars.Add((char)v);
                continue;
            }
            return new string(chars.ToArray());
        }
    }
}
