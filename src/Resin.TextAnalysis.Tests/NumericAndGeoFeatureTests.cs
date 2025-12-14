using MathNet.Numerics.LinearAlgebra;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class NumericAndGeoFeatureTests
    {
        private static Vector<double> ComposeDocumentVector(StringAnalyzer analyzer, string text)
        {
            var tokens = analyzer.Tokenize(new[] { text }).ToArray();
            Assert.IsTrue(tokens.Length > 0, "Expected at least one token for input.");

            var dim = tokens[0].vector.Count;
            var sum = CreateVector.Sparse<double>(dim);
            foreach (var t in tokens)
            {
                sum += t.vector;
            }

            var norm = sum.L2Norm();
            if (norm > 0) sum /= norm;

            return sum;
        }

        [TestMethod]
        public void GeoFeature_IncreasesSimilarityForCoordinatePairs()
        {
            var analyzer = new StringAnalyzer(numOfDimensions: 256);

            // Compose per-input vectors; each coordinate pair is split into tokens internally
            var nyc = ComposeDocumentVector(analyzer, "40.7128,-74.0060");      // NYC
            var la = ComposeDocumentVector(analyzer, "34.0522 -118.2437");     // LA

            var hello = ComposeDocumentVector(analyzer, "hello");

            var gSim = nyc.CosAngle(la);
            var gToW = nyc.CosAngle(hello);

            Assert.IsTrue(gSim > gToW, "Geo feature should increase similarity between coordinate tokens.");
        }

        [TestMethod]
        public void GeoFeature_DetectsSingleCoordinateWithHemisphere()
        {
            var analyzer = new StringAnalyzer(numOfDimensions: 256);

            var lat = ComposeDocumentVector(analyzer, "51.5074N");  // London latitude
            var lon = ComposeDocumentVector(analyzer, "0.1278W");   // London longitude
            var word = ComposeDocumentVector(analyzer, "Paris");

            var geoSim = lat.CosAngle(lon);
            var geoToWord = lat.CosAngle(word);

            Assert.IsTrue(geoSim > geoToWord, "Geo feature should increase similarity between lat/lon coordinate components.");
        }

        [TestMethod]
        public void NumericAndGeoFeatures_DoNotDominateOtherStructure()
        {
            var analyzer = new StringAnalyzer(numOfDimensions: 256);

            var v1 = ComposeDocumentVector(analyzer, "1");
            var v2 = ComposeDocumentVector(analyzer, "1000000");

            var sim = v1.CosAngle(v2);

            Assert.IsTrue(sim > 0.1, "Numeric feature should contribute some similarity.");
            Assert.IsTrue(sim < 0.99, "Numeric feature should not dominate all other features.");
        }

        [TestMethod]
        public void GeoFeature_DMS_Heuristic_ProducesSimilarity()
        {
            var analyzer = new StringAnalyzer(numOfDimensions: 256);

            var lat = ComposeDocumentVector(analyzer, "40°42'51\"N");  // NYC latitude
            var lon = ComposeDocumentVector(analyzer, "74°00'21\"W");  // NYC longitude
            var rnd = ComposeDocumentVector(analyzer, "random");

            var geoSim = lat.CosAngle(lon);
            var geoToRnd = lat.CosAngle(rnd);

            Assert.IsTrue(geoSim > geoToRnd, "DMS geo tokens should be more similar to each other than to random words.");
        }
    }
}
