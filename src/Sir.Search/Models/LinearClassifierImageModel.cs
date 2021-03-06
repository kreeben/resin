﻿using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sir.Search
{
    public class LinearClassifierImageModel : DistanceCalculator, IModel<IImage>
    {
        public double IdenticalAngle => 0.95d;
        public double FoldAngle => 0.75d;
        public override int NumOfDimensions => 784; 

        public void ExecutePut<T>(VectorNode column, VectorNode node)
        {
            column.MergeOrAddSupervised(node, this);
        }

        public IEnumerable<IVector> Tokenize(IImage data)
        {
            var pixels = data.Pixels.Select(x => Convert.ToSingle(x));

            yield return new IndexedVector(pixels, data.Label);
        }
    }
}