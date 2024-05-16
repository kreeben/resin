namespace Sir.Images
{
    public interface IImage : IByteStream
    {
        byte[] Pixels { get; }
        string Label { get; }
    }
}
