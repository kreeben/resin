namespace Resin.DataSources
{
    public interface IDataSource
    {
        IEnumerable<(string key, IEnumerable<string> values)> GetData(HashSet<string> fields);
    }
}
