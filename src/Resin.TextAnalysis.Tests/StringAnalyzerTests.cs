using MathNet.Numerics.LinearAlgebra;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.DataSources;
using Resin.KeyValue;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class StringAnalyzerTests
    {
        string[] Data = new[]
        {
            "\0\0 A world of dew,\r\n\r\nAnd within [every} §dewdrop.\r\n\r\nA world of struggle.",
            "I write, erase, rewrite\r\n\r\nErase \0\0again, and then\r\n\r\nA poppy blooms.",
            "a camera’s flash\r\n\r\nlong after the eyes close\r\n\r\nin old photographs",
            "Against his coat\r\n\r\nI brush my lips—\r\n\r\nthe \0\0silence of snowflakes",
            "all the skaters gone:\r\n\r\nthinner now the midnight ice\r\n\r\nacross \0\0the wide lake",
            "Martin Luther King\r\nDancing on his mountaintop\r\nLove raining on earth\r\nrosa’s haiku…",
            "An old silent pond...\r\nA frog jumps into the pond—\r\nSplash! Silence again.",
            "Autumn moonlight—\r\nA worm digs silently\r\nInto the chestnut.",
            "In the cicada's cry\r\nNo sign can foretell\r\nHow soon it must die.",
            "Over the wintry\r\nForest, winds howl in rage\r\nWith no leaves to blow.",
            "The words of children\r\nsplash in grandmother’s mind\r\nand ripple to now.",
            "Light of the moon\r\nMoves west, flowers' shadows\r\nCreep eastward.",
            "A field of cotton—\r\nAs if the moon\r\nHad flowered.",
            "Winter seclusion—\r\nListening, that evening,\r\nTo the rain in the mountain.",
            "From time to time\r\nThe clouds give rest\r\nTo the moon-beholders.",
            "The light of a candle\r\nIs transferred to another candle—\r\nSpring twilight",
            "A lovely sunset\r\nFor a brief moment\r\nBrings us together.",
            "The first soft snow\r\nFalling\r\nInto the basket."
        };

        [TestMethod]
        public void CanBuildAndValidateLexiconWithSyntheticData()
        {
            using (var session = new WriteSession())
            using (var readSession = new ReadSession(session))
            {
                var analyzer = new StringAnalyzer();
                analyzer.BuildLexicon(Data, session);
                Assert.IsTrue(analyzer.ValidateLexicon(Data, readSession));
            }
        }

        [TestMethod]
        public void CanBuildAndValidateLexiconWithWikipediaData()
        {
            var dataSource = new WikipediaCirrussearchDataSource(@"d:\enwiki-20211122-cirrussearch-content.json.gz")
                .GetData(new HashSet<string> { "text" });

            // Materialize once to ensure identical input to build and validate
            var data = dataSource
                .First().values
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Take(100)
                .ToArray();

            Assert.IsTrue(data.Any(), "Wikipedia data source returned no usable content.");

            using (var session = new WriteSession())
            using (var readSession = new ReadSession(session))
            {
                var analyzer = new StringAnalyzer(numOfDimensions: 2048);
                analyzer.BuildLexicon(data, session);
                var result = analyzer.ValidateLexicon(data, readSession);
                Assert.IsTrue(result);
            }
        }

        [TestMethod]
        public void CanTokenize()
        {
            var analyzer = new StringAnalyzer();
            Assert.IsTrue(analyzer.TokenizeIntoDouble(Data).Any());
        }

        [TestMethod]
        public void CanSerializeAndDeserializeVectorFloatValues()
        {
            const int pageSize = 4096;
            const int numOfDimensions = 512;
            using (var tx = new WriteSession(pageSize))
            using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx)))
            {
                var analyzer = new StringAnalyzer();
                var tokens = analyzer.TokenizeIntoFloat(new[] { "Resin" });
                var vector = tokens.First().vector;
                var unitVector = CreateVector.Sparse<float>(numOfDimensions, (float)1);
                var angle = unitVector.CosAngle(vector);
                var vectorBuf = vector.GetBytes(x => BitConverter.GetBytes(x), sizeof(float));
                pageWriter.TryPut(angle, vectorBuf);
                pageWriter.Serialize();

                using (var readSession = new ReadSession(tx))
                {
                    var tokenReader = new ColumnReader<double>(readSession);
                    var buf = tokenReader.Get(angle);
                    var storedVector = buf.ToArray().ToVectorFloat(numOfDimensions);
                    var a = vector.CosAngle(storedVector);

                    Assert.IsTrue(a > 0.99);
                }
            }
        }

        [TestMethod]
        public void CanSerializeAndDeserializeVectorDoubleValues()
        {
            const int pageSize = 4096;
            const int numOfDimensions = 512;
            using (var tx = new WriteSession(pageSize))
            using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx)))
            {
                var analyzer = new StringAnalyzer();
                var tokens = analyzer.TokenizeIntoDouble(new[] { "Resin" });
                var vector = tokens.First().vector;
                var unitVector = CreateVector.Sparse<double>(numOfDimensions, (double)1);
                var angle = unitVector.CosAngle(vector);
                var vectorBuf = vector.GetBytes(x => BitConverter.GetBytes(x), sizeof(double));
                pageWriter.TryPut(angle, vectorBuf);
                pageWriter.Serialize();

                using (var readSession = new ReadSession(tx))
                {
                    var tokenReader = new ColumnReader<double>(readSession);
                    var buf = tokenReader.Get(angle);
                    var storedVector = buf.ToArray().ToVectorDouble(numOfDimensions);
                    var a = vector.CosAngle(storedVector);

                    Assert.IsTrue(a > 0.99);
                }
            }
        }
    }
}
