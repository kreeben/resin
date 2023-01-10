using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace Sir
{
    public class SerializableVector : ISerializableVector
    {
        public object Label { get; private set; }
        public Vector<float> Value { get; private set; }
        public int ComponentCount => ((SparseVectorStorage<float>)Value.Storage).ValueCount;
        public int[] Indices { get { return ((SparseVectorStorage<float>)Value.Storage).Indices; } }
        public float[] Values { get { return ((SparseVectorStorage<float>)Value.Storage).Values; } }

        public SerializableVector(int numOfDimensions, object label = null)
        {
            Value = CreateVector.Sparse<float>(numOfDimensions);
            Label = label;
        }

        public SerializableVector(IEnumerable<float> values, string label = null)
        {
            Value = CreateVector.Sparse(SparseVectorStorage<float>.OfEnumerable(values));
            Label = label;
        }

        public SerializableVector(SortedList<int, float> dictionary, int numOfDimensions, object label = null)
        {
            var len = Math.Min(dictionary.Count, numOfDimensions);
            var tuples = ArrayPool<Tuple<int, float>>.Shared.Rent(len);
            var i = 0;

            foreach (var p in dictionary)
            {
                if (i == numOfDimensions)
                    break;

                tuples[i++] = new Tuple<int, float>(p.Key, p.Value);
            }

            Value = CreateVector.SparseOfIndexed(numOfDimensions, new ArraySegment<Tuple<int, float>>(tuples, 0, len));
            ArrayPool<Tuple<int, float>>.Shared.Return(tuples);
            Label = label;
        }

        public SerializableVector(int[] index, float[] values, int numOfDimensions, object label = null)
        {
            var tuples = new Tuple<int, float>[Math.Min(index.Length, numOfDimensions)];

            for (int i = 0; i < index.Length; i++)
            {
                if (i == numOfDimensions)
                    break;

                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            Value = CreateVector.Sparse(
                SparseVectorStorage<float>.OfIndexedEnumerable(numOfDimensions, tuples));

            Label = label;
        }

        public SerializableVector(Tuple<int, float>[] tuples, int numOfDimensions, object label = null)
        {
            Value = CreateVector.SparseOfIndexed(numOfDimensions, tuples);
            Label = label;
        }

        public SerializableVector(Vector<float> vector, object label = null)
        {
            Value = vector;
            Label = label;
        }

        public void AddInPlace(ISerializableVector vector)
        {
            Value = Value.Add(vector.Value);
            Label = $"{Label} {vector.Label}";
        }

        public ISerializableVector Add(ISerializableVector vector)
        {
            return new SerializableVector(Value.Add(vector.Value), Label);
        }

        public void SubtractInPlace(ISerializableVector vector)
        {
            Value.Subtract(vector.Value, Value);

            Value.CoerceZero(0);
        }

        public ISerializableVector Subtract(ISerializableVector vector)
        {
            return new SerializableVector(Value.Subtract(vector.Value), Label);
        }

        public ISerializableVector Multiply(float scalar)
        {
            var newVector = Value.Multiply(scalar);
            return new SerializableVector(newVector, Label);
        }

        public ISerializableVector Divide(float scalar)
        {
            var newVector = Value.Divide(scalar);
            return new SerializableVector(newVector, Label);
        }

        public void AverageInPlace(ISerializableVector vector)
        {
            Value.Add(vector.Value, Value);
            Value.Divide(2, Value);
        }

        public ISerializableVector Append(ISerializableVector vector)
        {
            var storage = (SparseVectorStorage<float>)vector.Value.Storage;
            var indices = storage.Indices;
            var shift = Value.Count;
            var numOfDims = Value.Count * 2;

            for (int i = 0; i < indices.Length; i++)
            {
                indices[i] += shift;
            }

            return new SerializableVector(Indices, Values, numOfDims, Label)
                .Add(new SerializableVector(indices, storage.Values, numOfDims, Label));
        }

        public ISerializableVector Shift(int numOfPositionsToShift, int numOfDimensions)
        {
            var storage = (SparseVectorStorage<float>)Value.Storage;
            var indices = (int[])storage.Indices.Clone();
            
            if (numOfPositionsToShift > 0)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    indices[i] += numOfPositionsToShift;
                }
            }

            return new SerializableVector(indices, (float[])Values.Clone(), numOfDimensions, Label);
        }

        public static ISerializableVector Deserialize(long vectorOffset, int componentCount, int vectorWidth, MemoryMappedViewAccessor vectorView)
        {
            if (vectorView == null)
            {
                throw new ArgumentNullException(nameof(vectorView));
            }

            var index = new int[componentCount];
            var values = new float[componentCount];

            var read = vectorView.ReadArray(vectorOffset, index, 0, index.Length);

            if (read < componentCount)
                throw new Exception("bad");

            read = vectorView.ReadArray(vectorOffset + (componentCount * sizeof(int)), values, 0, values.Length);

            if (read < componentCount)
                throw new Exception("bad");

            return new SerializableVector(index, values, vectorWidth, null);
        }

        public static ISerializableVector Deserialize(long vectorOffset, int componentCount, int numOfDimensions, Stream vectorStream)
        {
            Span<byte> buf = new byte[componentCount * 2 * sizeof(float)];

            vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, float>(buf.Slice(componentCount * sizeof(float)));
            var tuples = new Tuple<int, float>[componentCount];

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            return new SerializableVector(tuples, numOfDimensions);
        }

        public void Serialize(Stream stream)
        {
            var storage = (SparseVectorStorage<float>)Value.Storage;

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
        }

        public static long Serialize(ISerializableVector vector, Stream vectorStream)
        {
            var pos = vectorStream.Position;

            vector.Serialize(vectorStream);

            return pos;
        }

        public override string ToString()
        {
            return Label == null ? Value?.ToString() : Label.ToString();
        }
    }
}