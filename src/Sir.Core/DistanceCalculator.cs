namespace Sir
{
    public abstract class DistanceCalculator : IDistanceCalculator
    {
        public abstract int NumOfDimensions { get; }

        public double CosAngle(ISerializableVector vec1, ISerializableVector vec2)
        {
            var dotProduct = vec1.Value.DotProduct(vec2.Value);

            if (dotProduct == 0)
                return 0;

            var dotSelf1 = vec1.Value.Norm(2);
            var dotSelf2 = vec2.Value.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }
    }
}
