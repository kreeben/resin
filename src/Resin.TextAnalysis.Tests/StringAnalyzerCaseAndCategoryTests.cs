using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class StringAnalyzerCaseAndCategoryTests
    {
        [TestMethod]
        public void MixedTokenStream_AppliesCaseFlagsPerToken()
        {
            var sa = new StringAnalyzer(512);
            var tokens = sa.TokenizeIntoDouble("THE the The").ToArray();

            Assert.AreEqual(3, tokens.Length, "Expect three tokens");

            var v0 = tokens[0].vector; // THE
            var v1 = tokens[1].vector; // the
            var v2 = tokens[2].vector; // The

            int upperIdx = HashToIndex("case:upper", 512);
            int lowerIdx = HashToIndex("case:lower", 512);
            int titleIdx = HashToIndex("case:title", 512);

            Assert.IsTrue(v0[upperIdx] > 0.49 && v0[upperIdx] < 0.51, "Upper flag set for 'THE'");
            Assert.IsTrue(v1[lowerIdx] > 0.49 && v1[lowerIdx] < 0.51, "Lower flag set for 'the'");
            Assert.IsTrue(v2[titleIdx] > 0.49 && v2[titleIdx] < 0.51, "Title flag set for 'The'");
        }

        private static int HashToIndex(string key, int dims)
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            ulong h = offset;
            foreach (var ch in key)
            {
                h ^= ch;
                h *= prime;
            }
            return (int)(h % (ulong)dims);
        }
    }
}