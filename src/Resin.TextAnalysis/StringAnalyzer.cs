using System.Diagnostics;
using System.Globalization;
using System.Text.Unicode;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using Microsoft.Extensions.Logging;
using Resin.KeyValue;

namespace Resin.TextAnalysis
{
    public class StringAnalyzer
    {
        private readonly int _numOfDimensions;
        private readonly Vector<double> _unitVector;
        private readonly HashSet<UnicodeCategory> _validData = new HashSet<UnicodeCategory>
        {
            // punctuation
            UnicodeCategory.ClosePunctuation, UnicodeCategory.OpenPunctuation, UnicodeCategory.ConnectorPunctuation, UnicodeCategory.DashPunctuation, UnicodeCategory.FinalQuotePunctuation, UnicodeCategory.InitialQuotePunctuation, UnicodeCategory.OtherPunctuation,
            //letters
            UnicodeCategory.UppercaseLetter, UnicodeCategory.LowercaseLetter, UnicodeCategory.LetterNumber, UnicodeCategory.ModifierLetter, UnicodeCategory.TitlecaseLetter, UnicodeCategory.OtherLetter,
            //numbers and symbols
            UnicodeCategory.CurrencySymbol, UnicodeCategory.DecimalDigitNumber, UnicodeCategory.MathSymbol, UnicodeCategory.ModifierSymbol, UnicodeCategory.OtherNumber, UnicodeCategory.OtherSymbol
        };

        public StringAnalyzer(int numOfDimensions = 512)
        {
            _numOfDimensions = numOfDimensions;

            // Normalize the reference vector to a true unit vector (all-ones normalized).
            var ones = CreateVector.Dense<double>(_numOfDimensions, 1.0);
            var norm = ones.L2Norm();
            _unitVector = ones / norm;
        }

        public void BuildLexicon(IEnumerable<string> source, WriteSession tx, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
            {
                long docCount = 0;
                long tokenCount = 0;

                // Collect all (angle, buffer) pairs to allow sorting by angle for better write locality.
                var batches = new List<(double angle, byte[] buf)>(capacity: 4096);

                foreach (var str in source)
                {
                    foreach (var token in TokenizeIntoDouble(str))
                    {
                        var idVec = token.vector.Analyze(_unitVector);
                        var angleOfId = idVec.CosAngle(_unitVector);

                        // Use ArrayPool to avoid repeated temporary allocations when preparing buffers.
                        var tmp = token.vector.GetBytes((double d) => BitConverter.GetBytes(d), sizeof(double));
                        var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(tmp.Length);
                        Buffer.BlockCopy(tmp, 0, rented, 0, tmp.Length);

                        // Store the rented buffer; it will be written then returned to the pool.
                        batches.Add((angleOfId, rented));
                    }
                    docCount++;
                    log?.LogInformation($"doc count: {docCount} tokens (queued): {batches.Count}");
                    Debug.WriteLine($"doc count: {docCount} tokens (queued): {batches.Count}");
                }

                // Sort by angle to improve write locality and reduce random access during storage.
                batches.Sort((a, b) => a.angle.CompareTo(b.angle));

                foreach (var (angle, buf) in batches)
                {
                    // Write from the pooled buffer, then return it to the pool to minimize GC pressure.
                    if (writer.TryPut(angle, buf))
                    {
                        tokenCount++;
                    }
                    // Return the buffer after use.
                    System.Buffers.ArrayPool<byte>.Shared.Return(buf);
                }

                log?.LogInformation($"completed: docs={docCount} tokens (written): {tokenCount}");
                writer.Serialize();
            }
        }

        public bool ValidateLexicon(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession);
            var docCount = 0;
            foreach (var str in source)
            {
                double lowestAngleCollision = 1;
                string leastEntropicToken = string.Empty;
                foreach (var token in TokenizeIntoDouble(str))
                {
                    var idVec = token.vector.Analyze(_unitVector);
                    var angleOfId = idVec.CosAngle(_unitVector);
                    var tokenBuf = tokenReader.Get(angleOfId);

                    if (tokenBuf.IsEmpty)
                    {
                        throw new InvalidOperationException($"could not find '{token.label}' at {angleOfId}");
                    }

                    var tokenVec = tokenBuf.ToVectorDouble(_numOfDimensions);
                    double mutualAngle = tokenVec.CosAngle(token.vector);
                    if (mutualAngle < lowestAngleCollision)
                    {
                        lowestAngleCollision = mutualAngle;
                        leastEntropicToken = token.label;
                    }
                    if (mutualAngle < 0.9)
                    {
                        throw new InvalidOperationException($"collision for '{token.label}' at {angleOfId} mutualAngle:{mutualAngle}");
                    }
                }
                Debug.WriteLine($"lowestAngleCollision:{lowestAngleCollision} {leastEntropicToken}");
                log?.LogInformation($"lowestAngleCollision:{lowestAngleCollision} {leastEntropicToken}");


                docCount++;
                if (log != null)
                {
                    var headLen = Math.Min(25, str.Length);
                    var head = str.Substring(0, headLen);
                    var tailLen = Math.Min(25, str.Length);
                    var tailStart = Math.Max(0, str.Length - tailLen);
                    var tail = str.Substring(tailStart, str.Length - tailStart);
                    log.LogInformation($"VALID: doc {docCount} content: {head}...{tail}");
                }
            }

            return true;
        }

        private bool IsData(char c)
        {
            var cat = char.GetUnicodeCategory(c);
            if (_validData.Contains(cat))
                return true;
            return false;
        }

        // Deterministic char n-gram extraction to improve token stability.
        private static IEnumerable<string> GetCharNGrams(string s, int minN = 3, int maxN = 5)
        {
            if (string.IsNullOrEmpty(s))
                yield break;

            var len = s.Length;
            for (int n = minN; n <= maxN; n++)
            {
                for (int i = 0; i + n <= len; i++)
                {
                    yield return s.Substring(i, n);
                }
            }
        }

        // Simple deterministic hash to choose a dimension for n-gram features.
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

        // Adds lightweight case and Unicode category features for the label into the vector.
        // This increases entropy without changing existing positional and n-gram signals.
        private void AddCaseAndCategoryFeatures(string label, Vector<double> word)
        {
            if (string.IsNullOrEmpty(label))
                return;

            // Case features
            bool isAllLower = label.ToLowerInvariant() == label;
            bool isAllUpper = label.ToUpperInvariant() == label;
            bool isTitle = char.IsLetter(label[0]) && char.IsUpper(label[0]);

            // Hash dedicated feature keys into dimensions to avoid colliding with raw n-grams.
            void bump(string featureKey, double weight)
            {
                var d = HashToIndex(featureKey, _numOfDimensions);
                word[d] += weight;
            }

            bump(isAllLower ? "case:lower" : "case:mixed", 0.5);
            if (isAllUpper) bump("case:upper", 0.5);
            if (isTitle) bump("case:title", 0.5);

            // Unicode category distribution across label characters
            foreach (var ch in label)
            {
                var cat = char.GetUnicodeCategory(ch);
                var key = "uc:" + (int)cat;
                var d = HashToIndex(key, _numOfDimensions);
                word[d] += 0.25;
            }
        }

        // Split input into words using the same boundary rules as IsData (no ToCharArray to reduce allocations).
        private static List<string> SplitWords(string source, Func<char, bool> isData)
        {
            var words = new List<string>();
            var buf = new List<char>(64);

            foreach (var c in source)
            {
                if (isData(c))
                {
                    buf.Add(c);
                }
                else if (c == '’')
                {
                    // Keep behavior consistent with existing code (special apostrophe ignored).
                    var _ = char.GetUnicodeCategory(c);
                }
                else
                {
                    if (buf.Count > 0)
                    {
                        words.Add(new string(buf.ToArray()));
                        buf.Clear();
                    }
                }
            }

            if (buf.Count > 0)
            {
                words.Add(new string(buf.ToArray()));
                buf.Clear();
            }

            return words;
        }

        public IEnumerable<(string label, Vector<float> vector)> TokenizeIntoFloat(string source, bool labelVectors = true)
        {
            var words = SplitWords(source, IsData);
            foreach (var label in words)
            {
                // Build vector from the word label using original positional scheme.
                var word = CreateVector.Sparse<float>(_numOfDimensions);

                int index = 0;
                foreach (var c in label)
                {
                    if (index >= _numOfDimensions) break;
                    word[index] = c;
                    index++;
                }

                if (((SparseVectorStorage<float>)word.Storage).Values.Length > 0 && label.Length > 0)
                {
                    if (labelVectors)
                        yield return (label, word);
                    else
                        yield return (string.Empty, word);
                }
            }
        }

        public IEnumerable<(string label, Vector<double> vector)> TokenizeIntoDouble(string source, bool labelVectors = true)
        {
            var words = SplitWords(source, IsData);
            foreach (var label in words)
            {
                var word = CreateVector.Sparse<double>(_numOfDimensions);

                // Base character contribution using position (kept for backward compatibility)
                int index = 0;
                foreach (var c in label)
                {
                    if (index >= _numOfDimensions) break;
                    word[index] = c;
                    index++;
                }

                // Deterministic char n-gram features to stabilize the word vector.
                foreach (var ng in GetCharNGrams(label))
                {
                    var dim = HashToIndex(ng, _numOfDimensions);
                    word[dim] += 1.0;
                }

                // Case and Unicode category features
                AddCaseAndCategoryFeatures(label, word);

                if (((SparseVectorStorage<double>)word.Storage).Values.Length > 0 && label.Length > 0)
                {
                    if (labelVectors)
                        yield return (label, word);
                    else
                        yield return (string.Empty, word);
                }
            }
        }

        public IEnumerable<(string label, Vector<float> vector)> TokenizeIntoFloat(IEnumerable<string> source)
        {
            foreach (var str in source)
            {
                foreach (var token in TokenizeIntoFloat(str))
                {
                    yield return token;
                }
            }
        }

        public IEnumerable<(string label, Vector<double> vector)> TokenizeIntoDouble(IEnumerable<string> source)
        {
            foreach (var str in source)
            {
                foreach (var token in TokenizeIntoDouble(str))
                {
                    yield return token;
                }
            }
        }

        public UnicodeRange FindUnicodeRange(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            int first = 0;
            int last = 0;
            int count = 0;

            foreach (var str in source)
            {
                foreach (var c in str.ToCharArray())
                {
                    if (c > last)
                    {
                        last = c;
                    }
                    else if (c < last && c < first)
                    {
                        first = c;
                    }
                }
                if (log != null)
                    log.LogInformation($"{count++} {first} {last}");
            }

            return new UnicodeRange(first, last - first);
        }

        public double Compare(string str1, string str2)
        {
            var tokens = TokenizeIntoDouble(new[] { str1, str2 });
            var angle = tokens.First().vector.CosAngle(tokens.Last().vector);
            return angle;
        }

        public double CompareToUnitVector(string str1)
        {
            var tokens = TokenizeIntoDouble(new[] { str1 });
            var angle = tokens.First().vector.CosAngle(_unitVector);
            return angle;
        }
    }
}
