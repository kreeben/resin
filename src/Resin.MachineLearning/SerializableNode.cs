using MessagePack;

namespace Resin.MachineLearning
{
    [MessagePackObject]
    public class SerializableNode
    {
        [Key(0)]
        public string? Label { get; set; }

        [Key(1)]
        public int[]? SparseIndices { get; set; }

        [Key(2)]
        public float[]? SparseValues { get; set; }

        [Key(3)]
        public int LeftIndex { get; set; } = -1;

        [Key(4)]
        public int RightIndex { get; set; } = -1;
    }
}
