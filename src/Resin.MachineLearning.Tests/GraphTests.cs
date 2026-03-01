using MathNet.Numerics.LinearAlgebra;

namespace Resin.MachineLearning.Tests
{
    [TestClass]
    public sealed class GraphTests
    {
        [TestMethod]
        public void Save_and_Load_roundtrips_graph()
        {
            // Arrange
            var words = new[] { "sentence-A", "sentence-B", "sentence-C" };
            var options = new GraphOptions { IdenticalAngle = 0.99d, FoldAngle = 0.55d, Precision = 0.01d };
            var lexicon = new LexiconBuilder().BuildLexicon(words);
            var tokenizer = new Tokenizer(lexicon);
            var builder = new GraphBuilder(options, tokenizer);

            var sentences = new[] { "sentence-A;sentence-B", "sentence-B;sentence-C", "sentence-A;sentence-C" };
            var originalRoot = builder.BuildGraph(sentences);

            var serializer = new GraphSerializer();
            var filePath = Path.Combine(Path.GetTempPath(), $"graph_test_{Guid.NewGuid()}.msgpack");

            try
            {
                // Act
                serializer.Save(originalRoot, tokenizer.LexiconSize, filePath);
                var loadedRoot = serializer.Load(filePath);

                // Assert - validate by querying the loaded graph with the same data
                foreach (var sentence in sentences)
                {
                    var queryVector = Vector<float>.Build.Sparse(tokenizer.LexiconSize);
                    foreach (var token in tokenizer.Tokenize(sentence))
                    {
                        queryVector += token.Vector;
                    }

                    var hit = loadedRoot.ClosestMatch(queryVector, options);
                    Assert.IsTrue(
                        hit.Score.Approximates(options.IdenticalAngle, options.Precision) || hit.Score >= options.IdenticalAngle,
                        $"Expected score ~{options.IdenticalAngle} for '{sentence}', got {hit.Score:F4}");
                }
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
            }
        }

        [TestMethod]
        public void Save_and_Load_preserves_labels()
        {
            // Arrange
            var words = new[] { "X", "Y" };
            var lexicon = new LexiconBuilder().BuildLexicon(words);
            var tokenizer = new Tokenizer(lexicon);
            var options = new GraphOptions { IdenticalAngle = 0.99d, FoldAngle = 0.55d, Precision = 0.01d };
            var builder = new GraphBuilder(options, tokenizer);

            var originalRoot = builder.BuildGraph(["X;Y"]);

            var serializer = new GraphSerializer();
            var filePath = Path.Combine(Path.GetTempPath(), $"graph_label_test_{Guid.NewGuid()}.msgpack");

            try
            {
                // Act
                serializer.Save(originalRoot, tokenizer.LexiconSize, filePath);
                var loadedRoot = serializer.Load(filePath);

                // Assert - root label preserved
                Assert.AreEqual("root", loadedRoot.Token?.Label);

                // At least one child should have the sentence label
                var child = loadedRoot.Left ?? loadedRoot.Right;
                Assert.IsNotNull(child);
                Assert.AreEqual("X;Y", child.Token?.Label);
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
            }
        }

        [TestMethod]
        public void SeekableReader_ClosestMatch_finds_identical_scores()
        {
            // Arrange
            var words = new[] { "sentence-A", "sentence-B", "sentence-C" };
            var lexicon = new LexiconBuilder().BuildLexicon(words);
            var tokenizer = new Tokenizer(lexicon);
            var options = new GraphOptions { IdenticalAngle = 0.99d, FoldAngle = 0.55d, Precision = 0.01d };
            var builder = new GraphBuilder(options, tokenizer);

            var sentences = new[] { "sentence-A;sentence-B", "sentence-B;sentence-C", "sentence-A;sentence-C" };
            var originalRoot = builder.BuildGraph(sentences);

            var serializer = new GraphSerializer();
            var filePath = Path.Combine(Path.GetTempPath(), $"seekable_test_{Guid.NewGuid()}.msgpack");

            try
            {
                serializer.Save(originalRoot, tokenizer.LexiconSize, filePath);

                // Act - search via seekable reader (no full tree load)
                using var reader = new SeekableGraphReader(filePath);

                foreach (var sentence in sentences)
                {
                    var queryVector = Vector<float>.Build.Sparse(tokenizer.LexiconSize);
                    foreach (var token in tokenizer.Tokenize(sentence))
                    {
                        queryVector += token.Vector;
                    }

                    var hit = reader.ClosestMatch(queryVector, options);
                    Assert.IsTrue(
                        hit.Score.Approximates(options.IdenticalAngle, options.Precision) || hit.Score >= options.IdenticalAngle,
                        $"Expected score ~{options.IdenticalAngle} for '{sentence}', got {hit.Score:F4}");
                }
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
            }
        }

        [TestMethod]
        public void SeekableReader_ClosestMatch_matches_in_memory_search()
        {
            // Arrange
            var words = new[] { "sentence-A", "sentence-B", "sentence-C", "sentence-D" };
            var lexicon = new LexiconBuilder().BuildLexicon(words);
            var tokenizer = new Tokenizer(lexicon);
            var options = new GraphOptions { IdenticalAngle = 0.99d, FoldAngle = 0.55d, Precision = 0.01d };
            var builder = new GraphBuilder(options, tokenizer);

            var sentences = new[] { "sentence-A;sentence-B", "sentence-B;sentence-C", "sentence-C;sentence-D", "sentence-A;sentence-D" };
            var root = builder.BuildGraph(sentences);

            var serializer = new GraphSerializer();
            var filePath = Path.Combine(Path.GetTempPath(), $"seekable_match_test_{Guid.NewGuid()}.msgpack");

            try
            {
                serializer.Save(root, tokenizer.LexiconSize, filePath);

                using var reader = new SeekableGraphReader(filePath);

                foreach (var sentence in sentences)
                {
                    var queryVector = Vector<float>.Build.Sparse(tokenizer.LexiconSize);
                    foreach (var token in tokenizer.Tokenize(sentence))
                    {
                        queryVector += token.Vector;
                    }

                    var inMemoryHit = root.ClosestMatch(queryVector, options);
                    var seekableHit = reader.ClosestMatch(queryVector, options);

                    Assert.IsTrue(inMemoryHit.Score >= seekableHit.Score || inMemoryHit.Score.Approximates(seekableHit.Score, options.Precision));
                    Assert.AreEqual(inMemoryHit.Node.Token?.Label, seekableHit.Node.Token?.Label);
                }
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
            }
        }
    }
}
