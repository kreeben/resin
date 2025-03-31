using Sir.Wikipedia;

namespace Resin.DataSources
{
    /// <summary>
    /// https://dumps.wikimedia.org/other/cirrussearch/
    /// </summary>
    public class WikipediaCirrussearchDataSource : IDataSource
    {
        private readonly string _fileName;

        public WikipediaCirrussearchDataSource(string fileName)
        {
            _fileName = fileName ?? throw new ArgumentNullException(nameof(fileName));
        }

        public IEnumerable<string> GetData(string field)
        {
            return WikipediaHelper.Read(_fileName, 0, int.MaxValue, new HashSet<string> { field })
                .Select(x => x.First().value);
        }
    }
}
