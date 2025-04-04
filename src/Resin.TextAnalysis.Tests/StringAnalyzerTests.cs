using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.KeyValue;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class StringAnalyzerTests
    {
        string[] Data = new[]
        {
            "A world of dew,\r\n\r\nAnd within every dewdrop\r\n\r\nA world of struggle.",
            "I write, erase, rewrite\r\n\r\nErase again, and then\r\n\r\nA poppy blooms.",
            "a camera’s flash\r\n\r\nlong after the eyes close\r\n\r\nin old photographs",
            "Against his coat\r\n\r\nI brush my lips—\r\n\r\nthe silence of snowflakes",
            "all the skaters gone:\r\n\r\nthinner now the midnight ice\r\n\r\nacross the wide lake"
        };

        [TestMethod]
        public void CanBuildAndValidateLexicon()
        {
            using (var tx = new WriteTransaction())
            using (var readSession = new ReadSession(tx))
            {
                var analyzer = new StringAnalyzer();
                analyzer.BuildLexicon(Data, tx);
                analyzer.ValidateLexicon(Data, readSession);
            }
        }

        [TestMethod]
        public void CanTokenize()
        {
            var analyzer = new StringAnalyzer();
            var tokens = analyzer.Tokenize(Data);
        }
    }
}
