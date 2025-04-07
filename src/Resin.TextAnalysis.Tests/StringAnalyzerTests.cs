using MathNet.Numerics.LinearAlgebra;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.KeyValue;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class StringAnalyzerTests
    {
        string[] Data = new[]
        {
            "\0\0 A world of dew,\r\n\r\nAnd within [every} §dewdrop\r\n\r\nA world of struggle.",
            "I write, erase, rewrite\r\n\r\nErase \0\0again, and then\r\n\r\nA poppy blooms.",
            "a camera’s flash\r\n\r\nlong after the eyes close\r\n\r\nin old photographs",
            "Against his coat\r\n\r\nI brush my lips—\r\n\r\nthe \0\0silence of snowflakes",
            "all the skaters gone:\r\n\r\nthinner now the midnight ice\r\n\r\nacross \0\0the wide lake"
        };

        [TestMethod]
        public void CanBuildAndValidateLexicon()
        {
            using (var tx = new WriteTransaction())
            using (var readSession = new ReadSession(tx))
            {
                var analyzer = new StringAnalyzer();
                analyzer.BuildLexicon(Data, tx);
                Assert.IsTrue(analyzer.Validate(Data, readSession));
            }
        }

        [TestMethod]
        public void CanTokenize()
        {
            var analyzer = new StringAnalyzer();
            var tokens = analyzer.Tokenize(Data);
        }

        [TestMethod]
        public void CanSerializeAndDeserializeVectorValues()
        {
            const int pageSize = 4096;
            const int numOfDimensions = 512;
            using (var tx = new WriteTransaction())
            using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx, pageSize)))
            {
                var analyzer = new StringAnalyzer();
                var tokens = analyzer.Tokenize(new[] { "Resin" });
                var vector = tokens.First().vector;
                var unitVector = CreateVector.Sparse<float>(numOfDimensions, (float)1);
                var angle = VectorOperations.CosAngle(unitVector, vector);
                var vectorBuf = VectorOperations.GetBytes(vector);
                pageWriter.TryPut(angle, vectorBuf);
                pageWriter.Serialize();

                using (var readSession = new ReadSession(tx))
                {
                    var tokenReader = new ColumnReader<double>(readSession, sizeof(double), pageSize);
                    var buf = tokenReader.Get(angle);
                    var storedVector = VectorOperations.ToVector(buf.ToArray(), numOfDimensions);
                    var a = VectorOperations.CosAngle(vector, storedVector);

                    Assert.IsTrue(a > 0.99);
                }
            }
        }
    }
}
