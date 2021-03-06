﻿using System.Collections.Generic;
using System.IO;

namespace Sir.VectorSpace
{
    /// <summary>
    /// Vector space model.
    /// </summary>
    /// <typeparam name="T">The type of data the model should consist of.</typeparam>
    public interface IModel<T> : IModel
    {
        IEnumerable<IVector> Tokenize(T data);
    }

    /// <summary>
    /// Vector space model.
    /// </summary>
    public interface IModel : IVectorSpaceConfig, IDistanceCalculator, IIndexingStrategy
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
        double CosAngle(IVector vec1, IVector vec2);
        double CosAngle(IVector vector, long vectorOffset, int componentCount, Stream vectorStream);
    }
}
