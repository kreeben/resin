using MathNet.Numerics.LinearAlgebra;
using System.IO;

namespace Sir
{
    public interface ISerializableVector
    {
        int[] Indices { get; }
        float[] Values { get; }
        Vector<float> Value { get; }
        void Serialize(Stream stream);
        int ComponentCount { get; }
        object Label { get; }
        void AddInPlace(ISerializableVector vector);
        ISerializableVector Add(ISerializableVector vector);
        ISerializableVector Subtract(ISerializableVector vector);
        void SubtractInPlace(ISerializableVector vector);
        ISerializableVector Multiply(float scalar);
        ISerializableVector Divide(float scalar);
        void AverageInPlace(ISerializableVector vector);
        ISerializableVector Append(ISerializableVector vector);
        ISerializableVector Shift(int numOfPositionsToShift, int numOfDimensions);
    }
}