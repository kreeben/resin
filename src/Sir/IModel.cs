using System;
using System.Collections.Generic;
using System.IO;

namespace Sir
{
    /// <summary>
    /// String vector space model.
    /// </summary>
    public interface IStringModel : IModel<Memory<char>>
    {
    }

    public interface IModel<T> : IModel
    {
        IEnumerable<IVector> Tokenize(T data);
    }

    public interface IModel : IVectorSpaceConfig, IDistanceCalculator
    {
    }

    public interface IVectorSpaceConfig
    {
        int VectorWidth { get; }
        double FoldAngle { get; }
        double IdenticalAngle { get; }
    }

    public interface IDistanceCalculator
    {
        double CosAngle(IVector vec1, IVector vec2);
        double CosAngle(IVector vector, long vectorOffset, int componentCount, Stream vectorStream);
    }
}
