﻿using Sir.IO;
using Sir.Strings;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sir.Images
{
    public class LinearClassifierImageModel : DistanceCalculator, IModel<IImage>
    {
        public double IdenticalAngle => 0.95d;
        public double FoldAngle => 0.75d;
        public override int NumOfDimensions => 784; 

        public void ExecutePut<T>(VectorNode column, VectorNode node, IColumnReader reader)
        {
            column.AddOrAppendSupervised(node, this);
        }

        public IEnumerable<ISerializableVector> CreateEmbedding(IImage data, bool label)
        {
            var pixels = data.Pixels.Select(x => Convert.ToSingle(x));

            yield return new SerializableVector(pixels, data.Label);
        }
    }
}