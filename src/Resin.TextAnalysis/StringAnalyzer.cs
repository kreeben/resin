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
        private readonly double _identityAngle;
        private readonly HashSet<UnicodeCategory> _validData = new HashSet<UnicodeCategory>
        {
            // punctuation
            UnicodeCategory.ClosePunctuation, UnicodeCategory.OpenPunctuation, UnicodeCategory.ConnectorPunctuation, UnicodeCategory.DashPunctuation, UnicodeCategory.FinalQuotePunctuation, UnicodeCategory.InitialQuotePunctuation, UnicodeCategory.OtherPunctuation,
            //letters
            UnicodeCategory.UppercaseLetter, UnicodeCategory.LowercaseLetter, UnicodeCategory.LetterNumber, UnicodeCategory.ModifierLetter, UnicodeCategory.TitlecaseLetter, UnicodeCategory.OtherLetter,
            //numbers and symbols
            UnicodeCategory.CurrencySymbol, UnicodeCategory.DecimalDigitNumber, UnicodeCategory.MathSymbol, UnicodeCategory.ModifierSymbol, UnicodeCategory.OtherNumber, UnicodeCategory.OtherSymbol
        };

        public StringAnalyzer(int numOfDimensions = 512, double identityAngle = 0.9)
        {
            _numOfDimensions = numOfDimensions;

            // Normalize the reference vector to a all-ones normalized unit vector.
            var ones = CreateVector.Dense<double>(_numOfDimensions, 1.0);
            var norm = ones.L2Norm();
            _unitVector = ones / norm;
            _identityAngle = identityAngle;
        }

        public void BuildLexicon(IEnumerable<string> source, WriteSession tx, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            using (var writer = new ColumnWriter<double>(new PageWriter<double>(tx)))
            {
                long docCount = 0;
                long tokenCount = 0;

                // Collect all (angle, buffer) pairs to allow sorting by angle for better write locality.
                var batches = new List<(double angle, byte[] buf)>(capacity: 4096);

                foreach (var str in source)
                {
                    foreach (var token in TokenizeIntoVectors(str))
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
                    log?.LogInformation($"doc count: {docCount} tokens: {batches.Count}");
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

            var sw = Stopwatch.StartNew();
            var tokenReader = new ColumnReader<double>(readSession);
            var docCount = 0;
            long totalTokens = 0;
            double globalLowestAngle = 1.0;
            string globalLeastEntropicToken = string.Empty;

            foreach (var str in source)
            {
                double lowestAngleCollision = 1;
                string leastEntropicToken = string.Empty;
                int docTokenCount = 0;
                int collisionCount = 0;
                foreach (var token in TokenizeIntoVectors(str))
                {
                    docTokenCount++;
                    totalTokens++;

                    var idVec = token.vector.Analyze(_unitVector);
                    var angleOfId = idVec.CosAngle(_unitVector);
                    var tokenBuf = tokenReader.Get(angleOfId);

                    if (tokenBuf.IsEmpty)
                    {
                        log?.LogInformation($"could not find '{token.label}' at {angleOfId}");
                        return false;
                    }

                    var tokenVec = tokenBuf.ToVectorDouble(_numOfDimensions);
                    double mutualAngle = tokenVec.CosAngle(token.vector);
                    if (mutualAngle < lowestAngleCollision)
                    {
                        lowestAngleCollision = mutualAngle;
                        leastEntropicToken = token.label;
                    }
                    if (mutualAngle < _identityAngle)
                    {
                        collisionCount++;
                        log?.LogWarning($"collision for '{token.label}' mutualAngle:{mutualAngle}");

                        //throw new InvalidOperationException($"collision for '{token.label}' mutualAngle:{mutualAngle}");
                    }
                }

                if (lowestAngleCollision < globalLowestAngle)
                {
                    globalLowestAngle = lowestAngleCollision;
                    globalLeastEntropicToken = leastEntropicToken;
                }

                log?.LogInformation($"doc count: {docCount} tokens: {docTokenCount} total tokens: {totalTokens} collisions: {collisionCount} lowestAngleCollision:{lowestAngleCollision} {leastEntropicToken}");

                docCount++;

                // Periodic progress update every 50 docs or every ~2 seconds
                if (log != null)
                {
                    if (docCount % 50 == 0 || sw.ElapsedMilliseconds > 2000)
                    {
                        log.LogInformation("Progress: docs={DocCount}, tokens={TokenCount}, elapsed={Elapsed}",
                            docCount,
                            totalTokens,
                            sw.Elapsed);
                        sw.Restart();
                    }
                }
            }

            log?.LogInformation("ValidateLexicon: completed. docs={DocCount}, tokens={TokenCount}, minCollisionAngle={MinAngle} ({Token}), totalElapsed={Elapsed}",
                docCount,
                totalTokens,
                globalLowestAngle,
                string.IsNullOrEmpty(globalLeastEntropicToken) ? "(n/a)" : globalLeastEntropicToken,
                sw.Elapsed);

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

        // Split input into words using the same boundary rules as IsData.
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
                //else if (c == '’')
                //{
                //    // special apostrophe ignored.
                //    var _ = char.GetUnicodeCategory(c);
                //}
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

        // Position-aware bigrams and skip-grams (deterministic, low-cost).
        private static IEnumerable<string> GetPositionalBigrams(string s)
        {
            for (int i = 0; i + 1 < s.Length; i++)
            {
                yield return $"bg:{i}:{s[i]}{s[i + 1]}";
            }
        }

        private static IEnumerable<string> GetSkipGrams1(string s)
        {
            for (int i = 0; i + 2 < s.Length; i++)
            {
                yield return $"sg1:{i}:{s[i]}{s[i + 2]}";
            }
        }

        // Light trigram around boundaries to help tiny words and prefixes/suffixes.
        private static IEnumerable<string> GetBoundaryTrigrams(string s)
        {
            if (s.Length >= 3)
            {
                yield return $"tri:start:{s[0]}{s[1]}{s[2]}";
                yield return $"tri:end:{s[^3]}{s[^2]}{s[^1]}";
            }
            else if (s.Length == 2)
            {
                yield return $"tri:start:{s[0]}{s[1]}_";
                yield return $"tri:end:_{s[0]}{s[1]}";
            }
            else if (s.Length == 1)
            {
                yield return $"tri:start:{s[0]}__";
                yield return $"tri:end:__{s[0]}";
            }
        }

        private static bool IsVowel(char c)
        {
            switch (char.ToLowerInvariant(c))
            {
                case 'a':
                case 'e':
                case 'i':
                case 'o':
                case 'u':
                case 'y':
                    return true;
                default:
                    return false;
            }
        }

        private static string VcPattern(string s)
        {
            Span<char> buf = s.Length <= 64 ? stackalloc char[s.Length] : new char[s.Length];
            for (int i = 0; i < s.Length; i++)
            {
                buf[i] = char.IsLetter(s[i]) ? (IsVowel(s[i]) ? 'V' : 'C') : 'X';
            }
            return new string(buf);
        }

        // Rolling hash feature to stabilize very small tokens.
        private static ulong RollingHash64(string s)
        {
            const ulong seed = 11400714819323198485UL; // Knuth multiplicative
            ulong h = 0;
            foreach (var ch in s)
            {
                h = (h ^ ch) * seed;
            }
            return h;
        }

        public IEnumerable<(string label, Vector<double> vector)> TokenizeIntoVectors(string source, bool labelVectors = true)
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

                // Position-aware bigrams and skip-grams
                foreach (var k in GetPositionalBigrams(label))
                {
                    var d = HashToIndex(k, _numOfDimensions);
                    word[d] += 0.75;
                }
                foreach (var k in GetSkipGrams1(label))
                {
                    var d = HashToIndex(k, _numOfDimensions);
                    word[d] += 0.5;
                }

                // Boundary trigrams to help short words
                foreach (var k in GetBoundaryTrigrams(label))
                {
                    var d = HashToIndex(k, _numOfDimensions);
                    word[d] += 0.65;
                }

                // First/last character emphasis
                if (label.Length > 0)
                {
                    var firstKey = $"first:{label[0]}";
                    var lastKey = $"last:{label[^1]}";
                    word[HashToIndex(firstKey, _numOfDimensions)] += 0.75;
                    word[HashToIndex(lastKey, _numOfDimensions)] += 0.75;
                }

                // Token length buckets
                int len = label.Length;
                string bucket = len switch
                {
                    0 => "len:0",
                    1 => "len:1",
                    2 => "len:2",
                    3 => "len:3",
                    4 => "len:4",
                    <= 8 => "len:5-8",
                    <= 16 => "len:9-16",
                    _ => "len:17+"
                };
                word[HashToIndex(bucket, _numOfDimensions)] += 0.5;

                // Vowel/consonant pattern
                var pattern = VcPattern(label);
                word[HashToIndex("vc:" + pattern, _numOfDimensions)] += 0.5;

                // Rolling-hash anchored feature for tiny tokens
                if (len <= 3)
                {
                    var rh = RollingHash64(label);
                    var d = (int)(rh % (ulong)_numOfDimensions);
                    word[d] += 0.8;
                }

                // Case and Unicode category features
                AddCaseAndCategoryFeatures(label, word);

                // Optional normalization to reduce magnitude bias
                var storage = (SparseVectorStorage<double>)word.Storage;
                if (storage.Values.Length > 0 && len > 0)
                {
                    double l2 = word.L2Norm();
                    if (l2 > 0)
                    {
                        word /= l2;
                    }

                    if (labelVectors)
                        yield return (label, word);
                    else
                        yield return (string.Empty, word);
                }
            }
        }

        public IEnumerable<(string label, Vector<double> vector)> Tokenize(IEnumerable<string> source)
        {
            foreach (var str in source)
            {
                foreach (var token in TokenizeIntoVectors(str))
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
            var tokens = Tokenize(new[] { str1, str2 });
            var angle = tokens.First().vector.CosAngle(tokens.Last().vector);
            return angle;
        }

        public double CompareToUnitVector(string str1)
        {
            var tokens = Tokenize(new[] { str1 });
            var angle = tokens.First().vector.CosAngle(_unitVector);
            return angle;
        }
    }
}
