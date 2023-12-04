using Sir.IO;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sir.Images
{
    public class LinearClassifierImageModel : Sir.DistanceCalculator, IModel<IImage>
    {
        public double IdenticalAngle => 0.95d;
        public double FoldAngle => 0.75d;
        public override int NumOfDimensions => 784; 

        public IEnumerable<ISerializableVector> CreateEmbedding(IImage data, bool label, SortedList<int, float> embedding = null)
        {
            var pixels = data.Pixels.Select(x => Convert.ToSingle(x));

            yield return new SerializableVector(pixels, data.Label);
        }

        public Hit GetClosestMatchOrNull(ISerializableVector vector, IModel model, ColumnReader reader)
        {
            return reader.ClosestMatchOrNullScanningAllPages(vector, model);
        }
    }
}