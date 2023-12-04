using System.Collections.Generic;
using System.IO;

namespace Sir
{
    /// <summary>
    /// Vector space model.
    /// </summary>
    /// <typeparam name="T">The type of data the model should consist of.</typeparam>
    public interface IModel<T> : IModel
    {
        IEnumerable<ISerializableVector> CreateEmbedding(T data, bool label, SortedList<int, float> embedding = null);
    }

    /// <summary>
    /// Vector space model.
    /// </summary>
    public interface IModel : IVectorSpaceConfig, IDistanceCalculator
    {
    }

    /// <summary>
    /// Vector space configuration.
    /// </summary>
    public interface IVectorSpaceConfig
    {
        double FoldAngle { get; }
        double IdenticalAngle { get; }
    }

    /// <summary>
    /// Calculates the angle between two vectors.
    /// </summary>
    public interface IDistanceCalculator
    {
        int NumOfDimensions { get; }
        double CosAngle(ISerializableVector vec1, ISerializableVector vec2);
        double CosAngle(ISerializableVector vector, long vectorOffset, int componentCount, Stream vectorStream);
    }
}
