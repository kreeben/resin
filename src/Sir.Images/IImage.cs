namespace Sir.Images
{
    public interface IImage : IStreamable
    {
        byte[] Pixels { get; }
        string Label { get; }
    }
}
