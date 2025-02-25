using System.Text.Unicode;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.DataSources;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class StringAnalyzerTests
    {
        [TestMethod]
        public void CanGetUnicodeRange()
        {
            var dataSource = new WikipediaCirrussearchDataSource(@"d:\enwiki-20211122-cirrussearch-content.json.gz").GetData().Take(100);
            var unicodeRange = new StringAnalyzer().FindUnicodeRange(dataSource);

            Assert.IsTrue(unicodeRange.FirstCodePoint >= UnicodeRanges.All.FirstCodePoint);
            Assert.IsTrue(unicodeRange.FirstCodePoint + unicodeRange.Length <= UnicodeRanges.All.FirstCodePoint + UnicodeRanges.All.Length);
        }
    }
}
