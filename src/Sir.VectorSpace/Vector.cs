﻿using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.VectorSpace
{
    [System.Diagnostics.DebuggerDisplay("{Label}")]
    public class IndexedVector : IVector
    {
        public object Label { get; }
        public Vector<float> Value { get; private set; }
        public int ComponentCount => ((SparseVectorStorage<float>)Value.Storage).ValueCount;
        public int[] Indices { get { return ((SparseVectorStorage<float>)Value.Storage).Indices; } }
        public float[] Values { get { return ((SparseVectorStorage<float>)Value.Storage).Values; } }

        public IndexedVector(int numOfDimensions, object label = null)
        {
            Value = CreateVector.Sparse<float>(numOfDimensions);
            Label = label;
        }

        public IndexedVector(IEnumerable<float> values, string label = null)
        {
            Value = CreateVector.Sparse(SparseVectorStorage<float>.OfEnumerable(values));
            Label = label;
        }

        public IndexedVector(SortedList<int, float> dictionary, int numOfDimensions, object label = null)
        {
            var tuples = new Tuple<int, float>[Math.Min(dictionary.Count, numOfDimensions)];
            var i = 0;

            foreach (var p in dictionary)
            {
                if (i == numOfDimensions)
                    break;

                tuples[i++] = new Tuple<int, float>(p.Key, p.Value);
            }

            Value = CreateVector.SparseOfIndexed(numOfDimensions, tuples);
            Label = label;
        }

        public IndexedVector(int[] index, float[] values, int numOfDimensions, object label = null)
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

        public IndexedVector(Tuple<int, float>[] tuples, int numOfDimensions, object label = null)
        {
            Value = CreateVector.SparseOfIndexed(numOfDimensions, tuples);
            Label = label;
        }

        public IndexedVector(Vector<float> vector, object label = null)
        {
            Value = vector;
            Label = label;
        }

        public void Serialize(Stream stream)
        {
            var storage = (SparseVectorStorage<float>)Value.Storage;
            var indices = MemoryMarshal.Cast<int, byte>(storage.Indices);
            var values = MemoryMarshal.Cast<float, byte>(storage.Values);

            stream.Write(indices);
            stream.Write(values);
        }

        public void AddInPlace(IVector vector)
        {
            Value = Value.Add(vector.Value);
        }

        public IVector Add(IVector vector)
        {
            return new IndexedVector(Value.Add(vector.Value), Label);
        }

        public void SubtractInPlace(IVector vector)
        {
            Value.Subtract(vector.Value, Value);

            Value.CoerceZero(0);
        }

        public IVector Subtract(IVector vector)
        {
            return new IndexedVector(Value.Subtract(vector.Value), Label);
        }

        public IVector Multiply(float scalar)
        {
            var newVector = Value.Multiply(scalar);
            return new IndexedVector(newVector, Label);
        }

        public IVector Divide(float scalar)
        {
            var newVector = Value.Divide(scalar);
            return new IndexedVector(newVector, Label);
        }

        public void AverageInPlace(IVector vector)
        {
            Value.Add(vector.Value, Value);
            Value.Divide(2, Value);
        }

        public IVector Append(IVector vector)
        {
            var storage = (SparseVectorStorage<float>)vector.Value.Storage;
            var indices = storage.Indices;
            var shift = Value.Count;
            var numOfDims = Value.Count * 2;

            for (int i = 0; i < indices.Length; i++)
            {
                indices[i] += shift;
            }

            return new IndexedVector(Indices, Values, numOfDims, Label)
                .Add(new IndexedVector(indices, storage.Values, numOfDims, Label));
        }

        public IVector Shift(int numOfPositionsToShift, int numOfDimensions, string label = null)
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

            return new IndexedVector(indices, (float[])Values.Clone(), numOfDimensions, label??Label);
        }

        public override string ToString()
        {
            return Label.ToString();
        }
    }
}