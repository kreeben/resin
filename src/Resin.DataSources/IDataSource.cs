namespace Resin.DataSources
{
    public interface IDataSource
    {
        IEnumerable<string> GetData(string field);
    }
}
