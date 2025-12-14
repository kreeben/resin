using MathNet.Numerics.LinearAlgebra;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.DataSources;
using Resin.KeyValue;

namespace Resin.TextAnalysis.Tests
{
    [TestClass]
    public class StringAnalyzerTests
    {
        string[] SyntheticData = new[]
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
        public void SplitWords_RemovesStandalonePunctuation()
        {
            var analyzer = new StringAnalyzer(128);

            var input = new[]
            {
                "!!!",
                "???!",
                "... --- ...",
                "— — —",
                "()[]{}<>",
                "A lovely sunset!!!"
            };

            var tokens = analyzer.Tokenize(input).ToArray();

            // Standalone punctuation should not produce any tokens.
            // Only "A lovely sunset!!!" produces words, and punctuation is stripped.
            Assert.IsTrue(tokens.Length >= 3, "Expected tokens from the valid sentence only.");
            CollectionAssert.DoesNotContain(tokens.Select(t => t.label).ToArray(), "!!!", "Standalone punctuation should be removed.");
            CollectionAssert.DoesNotContain(tokens.Select(t => t.label).ToArray(), "???!", "Standalone punctuation should be removed.");
            CollectionAssert.DoesNotContain(tokens.Select(t => t.label).ToArray(), "...", "Standalone punctuation should be removed.");
            CollectionAssert.DoesNotContain(tokens.Select(t => t.label).ToArray(), "—", "Standalone punctuation should be removed.");

            // Ensure trailing punctuation is stripped from words
            var labels = tokens.Select(t => t.label).ToArray();
            Assert.IsTrue(labels.Contains("A"), "Expected 'A' token.");
            Assert.IsTrue(labels.Contains("lovely"), "Expected 'lovely' token.");
            Assert.IsTrue(labels.Contains("sunset"), "Expected 'sunset' token.");
        }

        [TestMethod]
        public void SplitWords_StripsInternalPunctuationFromWords()
        {
            var analyzer = new StringAnalyzer(128);

            var input = new[]
            {
                "children’s minds",
                "rock-n-roll",
                "email@example.com",
                "well...known",
                "C#/.NET"
            };

            var tokens = analyzer.Tokenize(input).ToArray();
            var labels = tokens.Select(t => t.label).ToArray();

            // Apostrophes, dashes, dots, slashes and punctuation are removed within words
            Assert.IsTrue(labels.Contains("children"), "Expected internal apostrophe to be removed: 'children’s' -> 'children'.");
            Assert.IsTrue(labels.Contains("s"), "Expected internal apostrophe to be removed: 'children’s' -> 's'.");
            Assert.IsTrue(labels.Contains("minds"), "Expected 'minds' token.");
            Assert.IsTrue(labels.Contains("rock"), "Expected hyphens to be removed: 'rock-n-roll' -> 'rock'.");
            Assert.IsTrue(labels.Contains("n"), "Expected hyphens to be removed: 'rock-n-roll' -> 'n'.");
            Assert.IsTrue(labels.Contains("roll"), "Expected hyphens to be removed: 'rock-n-roll' -> 'roll'.");
            Assert.IsTrue(labels.Contains("email"), "Expected punctuation removed: 'email@example.com' -> 'email'.");
            Assert.IsTrue(labels.Contains("example"), "Expected punctuation removed: 'email@example.com' -> 'example'.");
            Assert.IsTrue(labels.Contains("com"), "Expected punctuation removed: 'email@example.com' -> 'com'.");
            Assert.IsTrue(labels.Contains("well"), "Expected dots removed: 'well...known' -> 'well'.");
            Assert.IsTrue(labels.Contains("known"), "Expected dots removed: 'well...known' -> 'known'.");
            Assert.IsTrue(labels.Contains("C"), "Expected 'C' from 'C#/.NET'.");
            Assert.IsTrue(labels.Contains("NET"), "Expected 'NET' from 'C#/.NET'.");
        }

        [TestMethod]
        public void SplitWords_PreservesLettersDigitsAndSymbolsNonPunctuation()
        {
            var analyzer = new StringAnalyzer(128);

            var input = new[]
            {
                "abc123",
                "€money$",
                "Math≈Science",
                "A_b_c" // underscore is ConnectorPunctuation, should be removed
            };

            var tokens = analyzer.Tokenize(input).ToArray();
            var labels = tokens.Select(t => t.label).ToArray();

            Assert.IsTrue(labels.Contains("abc123"), "Digits and letters should remain.");
            Assert.IsTrue(labels.Contains("€money$"), "Currency symbol and dollar sign are treated as symbols and should remain if not classified as punctuation.");
            Assert.IsTrue(labels.Contains("Math≈Science"), "Math symbol '≈' should remain.");
            Assert.IsFalse(labels.Contains("A_b_c"), "Connector punctuation underscore should be removed.");
        }

        [TestMethod]
        public void CanBuildAndValidateLexiconWithSyntheticData()
        {
            using (var session = new WriteSession())
            using (var readSession = new ReadSession(session))
            {
                var analyzer = new StringAnalyzer();
                analyzer.BuildLexicon(SyntheticData, session);
                Assert.IsTrue(analyzer.ValidateLexicon(SyntheticData, readSession));
            }
        }

        [TestMethod]
        public void CanBuildAndValidateLexiconWithWikipediaData()
        {
            var dataSource = new WikipediaCirrussearchDataSource(@"d:\enwiki-20211122-cirrussearch-content.json.gz")
                .GetData(new HashSet<string> { "text" });

            var data = dataSource
                .First().values
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Take(100)
                .ToArray();

            Assert.IsTrue(data.Any(), "Wikipedia data source returned no usable content.");

            using (var session = new WriteSession())
            using (var readSession = new ReadSession(session))
            {
                var analyzer = new StringAnalyzer(numOfDimensions: 256, identityAngle: 0.9);
                analyzer.BuildLexicon(data, session);
                var result = analyzer.ValidateLexicon(data, readSession);
                Assert.IsTrue(result);
            }
        }

        [TestMethod]
        public void CanValidateSyntheticLexicon_PositiveAndNegativeCases()
        {
            using (var session = new WriteSession())
            using (var readSession = new ReadSession(session))
            {
                var analyzer = new StringAnalyzer(numOfDimensions: 64, identityAngle: 0.8);

                // Positive: build and validate against the same corpus
                analyzer.BuildLexicon(SyntheticData, session);
                Assert.IsTrue(analyzer.ValidateLexicon(SyntheticData, readSession), "Expected validation to succeed for known corpus.");

                // Negative: validate against out-of-lexicon tokens (intentionally different words)
                var unknownData = new[]
                {
                    "quantum entanglement",
                    "distributed ledger",
                    "neural radiance fields",
                    "gamma ray bursts",
                    "hyperbolic embeddings"
                };

                Assert.IsFalse(analyzer.ValidateLexicon(unknownData, readSession), "Expected validation to fail for unknown corpus.");
            }
        }

        [TestMethod]
        public void CanTokenize()
        {
            var analyzer = new StringAnalyzer(512);

            var input = new[]
            {
                "Resin resin RESIN",
                "",
                "   ",
                "\0\0",
                "!!!",
                "A lovely sunset!!!"
            };

            var tokens = analyzer.Tokenize(input).ToArray();

            Assert.IsTrue(tokens.Any(), "Expected at least one token from valid input.");

            // Updated: punctuation-only entries should not produce tokens anymore.
            var noiseOnly = new[] { string.Empty, "   ", "\0\0", "!!!" };
            var noiseTokens = analyzer.Tokenize(noiseOnly).ToArray();
            Assert.AreEqual(0, noiseTokens.Length, "Standalone punctuation should not produce tokens.");

            // Structural checks
            foreach (var t in tokens)
            {
                var v = t.vector;
                Assert.AreEqual(512, v.Count, "Token vector dimensionality mismatch.");
                for (int i = 0; i < v.Count; i++)
                {
                    var val = v[i];
                    Assert.IsFalse(double.IsNaN(val) || double.IsInfinity(val), $"Invalid value in vector at index {i}.");
                }
                var norm = v.L2Norm();
                Assert.IsTrue(norm > 0.0, "Vector norm must be positive.");
                Assert.IsTrue(norm < 1e6, "Vector norm is suspiciously large.");
            }

            var validOnly = new[] { "Resin resin RESIN", "A lovely sunset!!!" };
            var validTokens = analyzer.Tokenize(validOnly).ToArray();
            Assert.IsTrue(validTokens.Length >= 5, "Expected at least five tokens from valid input sample.");

            var unit = CreateVector.Sparse<double>(512, 1.0);
            foreach (var t in validTokens)
            {
                var angle = unit.CosAngle(t.vector);
                Assert.IsTrue(angle >= -1.0 && angle <= 1.0, "Cosine angle must be within [-1, 1].");
            }
        }

        [TestMethod]
        public void CanSerializeAndDeserializeVectorDoubleValues()
        {
            const int pageSize = 4096;
            const int numOfDimensions = 512;
            using (var tx = new WriteSession(pageSize))
            using (var pageWriter = new ColumnWriter<double>(new PageWriter<double>(tx)))
            {
                var analyzer = new StringAnalyzer();
                var tokens = analyzer.Tokenize(new[] { "Resin" });
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
                    var storedVector = buf.ToVectorDouble(numOfDimensions);
                    var a = vector.CosAngle(storedVector);

                    Assert.IsTrue(a > 0.99);
                }
            }
        }
    }
}
