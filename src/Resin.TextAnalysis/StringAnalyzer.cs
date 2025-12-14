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

        // Deterministic char n-gram extraction without allocations using spans.
        private static void AddCharNGramFeatures(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            if (s.Length == 0) return;
            for (int n = 3; n <= 5; n++)
            {
                for (int i = 0; i + n <= s.Length; i++)
                {
                    var d = HashToIndex(s.Slice(i, n), dims);
                    word[d] += 1.0;
                }
            }
        }

        // FNV-1a-like hash over spans to avoid string allocations
        private static int HashToIndex(ReadOnlySpan<char> key, int dims)
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            ulong h = offset;
            for (int i = 0; i < key.Length; i++)
            {
                h ^= key[i];
                h *= prime;
            }
            return (int)(h % (ulong)dims);
        }

        // Simple deterministic hash to choose a dimension for n-gram features (string overload retained for callers).
        private static int HashToIndex(string key, int dims)
        {
            return HashToIndex(key.AsSpan(), dims);
        }

        // Adds lightweight case and Unicode category features for the label into the vector.
        private void AddCaseAndCategoryFeatures(string label, Vector<double> word)
        {
            if (string.IsNullOrEmpty(label))
                return;

            // Case features
            bool isAllLower = label.ToLowerInvariant() == label;
            bool isAllUpper = label.ToUpperInvariant() == label;
            bool isTitle = char.IsLetter(label[0]) && char.IsUpper(label[0]);

            // bump via span without allocating
            void bump(ReadOnlySpan<char> featureKey, double weight)
            {
                var d = HashToIndex(featureKey, _numOfDimensions);
                word[d] += weight;
            }

            bump((isAllLower ? "case:lower" : "case:mixed").AsSpan(), 0.5);
            if (isAllUpper) bump("case:upper".AsSpan(), 0.5);
            if (isTitle) bump("case:title".AsSpan(), 0.5);

            // Unicode category distribution across label characters
            foreach (var ch in label)
            {
                // Build small key without allocating using stackalloc
                Span<char> small = stackalloc char[3]; // "uc:" + number (we'll hash 'uc:' and cat value separately)
                // Hash prefix
                int dPrefix = HashToIndex("uc:".AsSpan(), _numOfDimensions);
                var cat = (int)char.GetUnicodeCategory(ch);
                // Combine by hashing cat as two chars to vary dimension
                Span<char> catSpan = stackalloc char[2];
                catSpan[0] = (char)('0' + (cat % 10));
                catSpan[1] = (char)('0' + ((cat / 10) % 10));
                var d = HashToIndex(catSpan, _numOfDimensions);
                word[(d + dPrefix) % _numOfDimensions] += 0.25;
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

        // Position-aware bigrams and skip-grams without string allocations.
        private static void AddPositionalBigrams(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            for (int i = 0; i + 1 < s.Length; i++)
            {
                // Hash prefix and content
                int p = HashToIndex("bg:".AsSpan(), dims);
                Span<char> span = stackalloc char[4];
                span[0] = s[i];
                span[1] = s[i + 1];
                // encode position low byte to vary index slightly
                span[2] = (char)(i & 0xFF);
                span[3] = (char)((i >> 8) & 0xFF);
                int d = (p + HashToIndex(span, dims)) % dims;
                word[d] += 0.75;
            }
        }

        private static void AddSkipGrams1(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            for (int i = 0; i + 2 < s.Length; i++)
            {
                int p = HashToIndex("sg1:".AsSpan(), dims);
                Span<char> span = stackalloc char[4];
                span[0] = s[i];
                span[1] = s[i + 2];
                span[2] = (char)(i & 0xFF);
                span[3] = (char)((i >> 8) & 0xFF);
                int d = (p + HashToIndex(span, dims)) % dims;
                word[d] += 0.5;
            }
        }

        // Light trigram around boundaries without allocations.
        private static void AddBoundaryTrigrams(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            int pStart = HashToIndex("tri:start:".AsSpan(), dims);
            int pEnd = HashToIndex("tri:end:".AsSpan(), dims);
            if (s.Length >= 3)
            {
                Span<char> span = stackalloc char[3];
                span[0] = s[0]; span[1] = s[1]; span[2] = s[2];
                word[(pStart + HashToIndex(span, dims)) % dims] += 0.65;
                span[0] = s[s.Length - 3]; span[1] = s[s.Length - 2]; span[2] = s[s.Length - 1];
                word[(pEnd + HashToIndex(span, dims)) % dims] += 0.65;
            }
            else if (s.Length == 2)
            {
                Span<char> span = stackalloc char[3];
                span[0] = s[0]; span[1] = s[1]; span[2] = '_';
                word[(pStart + HashToIndex(span, dims)) % dims] += 0.65;
                span[0] = '_'; span[1] = s[0]; span[2] = s[1];
                word[(pEnd + HashToIndex(span, dims)) % dims] += 0.65;
            }
            else if (s.Length == 1)
            {
                Span<char> span = stackalloc char[3];
                span[0] = s[0]; span[1] = '_'; span[2] = '_';
                word[(pStart + HashToIndex(span, dims)) % dims] += 0.65;
                span[0] = '_'; span[1] = '_'; span[2] = s[0];
                word[(pEnd + HashToIndex(span, dims)) % dims] += 0.65;
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

        private static void AddVcPattern(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            if (s.Length == 0) return;
            Span<char> buf = s.Length <= 64 ? stackalloc char[s.Length] : new char[s.Length];
            for (int i = 0; i < s.Length; i++)
            {
                buf[i] = char.IsLetter(s[i]) ? (IsVowel(s[i]) ? 'V' : 'C') : 'X';
            }
            int p = HashToIndex("vc:".AsSpan(), dims);
            int d = (p + HashToIndex(buf, dims)) % dims;
            word[d] += 0.5;
        }

        // Rolling hash feature to stabilize very small tokens.
        private static ulong RollingHash64(ReadOnlySpan<char> s)
        {
            const ulong seed = 11400714819323198485UL; // Knuth multiplicative
            ulong h = 0;
            for (int i = 0; i < s.Length; i++)
            {
                h = (h ^ s[i]) * seed;
            }
            return h;
        }

        public IEnumerable<(string label, Vector<double> vector)> TokenizeIntoVectors(string source, bool labelVectors = true)
        {
            var words = SplitWords(source, IsData);
            foreach (var label in words)
            {
                var word = CreateVector.Sparse<double>(_numOfDimensions);
                ReadOnlySpan<char> labelSpan = label.AsSpan();

                // Base character contribution using position (kept for backward compatibility)
                int index = 0;
                foreach (var c in labelSpan)
                {
                    if (index >= _numOfDimensions) break;
                    word[index] = c;
                    index++;
                }

                // Deterministic char n-gram features without allocations
                AddCharNGramFeatures(labelSpan, _numOfDimensions, word);

                // Position-aware bigrams and skip-grams
                AddPositionalBigrams(labelSpan, _numOfDimensions, word);
                AddSkipGrams1(labelSpan, _numOfDimensions, word);

                // Boundary trigrams to help short words
                AddBoundaryTrigrams(labelSpan, _numOfDimensions, word);

                // First/last character emphasis
                if (labelSpan.Length > 0)
                {
                    int pFirst = HashToIndex("first:".AsSpan(), _numOfDimensions);
                    int pLast = HashToIndex("last:".AsSpan(), _numOfDimensions);
                    Span<char> cbuf = stackalloc char[1];
                    cbuf[0] = labelSpan[0];
                    word[(pFirst + HashToIndex(cbuf, _numOfDimensions)) % _numOfDimensions] += 0.75;
                    cbuf[0] = labelSpan[labelSpan.Length - 1];
                    word[(pLast + HashToIndex(cbuf, _numOfDimensions)) % _numOfDimensions] += 0.75;
                }

                // Token length buckets
                int len = labelSpan.Length;
                ReadOnlySpan<char> bucket = len switch
                {
                    0 => "len:0".AsSpan(),
                    1 => "len:1".AsSpan(),
                    2 => "len:2".AsSpan(),
                    3 => "len:3".AsSpan(),
                    4 => "len:4".AsSpan(),
                    <= 8 => "len:5-8".AsSpan(),
                    <= 16 => "len:9-16".AsSpan(),
                    _ => "len:17+".AsSpan()
                };
                word[HashToIndex(bucket, _numOfDimensions)] += 0.5;

                // Vowel/consonant pattern
                AddVcPattern(labelSpan, _numOfDimensions, word);

                // Rolling-hash anchored feature for tiny tokens
                if (len <= 3)
                {
                    var rh = RollingHash64(labelSpan);
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
