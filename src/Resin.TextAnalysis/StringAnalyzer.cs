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
                        var idVec = VectorOperations.Analyze(token.vector, _unitVector);
                        var angleOfId = VectorOperations.CosAngle(idVec, _unitVector);

                        // Use ArrayPool to avoid repeated temporary allocations when preparing buffers.
                        var tmp = VectorOperations.GetBytes(token.vector, (double d) => BitConverter.GetBytes(d), sizeof(double));
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

                    var idVec = VectorOperations.Analyze(token.vector, _unitVector);
                    var angleOfId = VectorOperations.CosAngle(idVec, _unitVector);
                    var tokenBuf = tokenReader.Get(angleOfId);

                    if (tokenBuf.IsEmpty)
                    {
                        log?.LogInformation($"could not find '{token.label}' at {angleOfId}");
                        return false;
                    }

                    var tokenVec = VectorOperations.ToVectorDouble(tokenBuf, _numOfDimensions);
                    double mutualAngle = VectorOperations.CosAngle(tokenVec, token.vector);
                    if (mutualAngle < lowestAngleCollision)
                    {
                        lowestAngleCollision = mutualAngle;
                        leastEntropicToken = token.label;
                    }
                    if (mutualAngle < _identityAngle)
                    {
                        collisionCount++;
                        log?.LogWarning($"doc count: {docCount} collision for '{token.label}' mutualAngle:{mutualAngle}");

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

            log?.LogWarning("ValidateLexicon: completed. docs={DocCount}, tokens={TokenCount}, minCollisionAngle={MinAngle}, token={Token}, totalElapsed={Elapsed}",
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
            int dPrefix = HashToIndex("uc:".AsSpan(), _numOfDimensions);
            Span<char> catSpan = stackalloc char[2];
            foreach (var ch in label)
            {
                var cat = (int)char.GetUnicodeCategory(ch);
                // Encode category as two chars and reuse the same buffer
                catSpan[0] = (char)('0' + (cat % 10));
                catSpan[1] = (char)('0' + ((cat / 10) % 10));
                var d = HashToIndex(catSpan, _numOfDimensions);
                word[(d + dPrefix) % _numOfDimensions] += 0.25;
            }
        }

        // Split input into words using the same boundary rules as IsData, but remove punctuation from emitted words.
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

            // Remove any empty strings that could result from punctuation-only segments
            if (words.Count > 0)
            {
                for (int i = words.Count - 1; i >= 0; i--)
                {
                    if (string.IsNullOrEmpty(words[i]))
                    {
                        words.RemoveAt(i);
                    }
                }
            }

            return words;
        }

        // Position-aware bigrams and skip-grams without string allocations.
        private static void AddPositionalBigrams(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            int p = HashToIndex("bg:".AsSpan(), dims);
            Span<char> span = stackalloc char[4];
            for (int i = 0; i + 1 < s.Length; i++)
            {
                // Reuse buffer for all iterations
                span[0] = s[i];
                span[1] = s[i + 1];
                span[2] = (char)(i & 0xFF);
                span[3] = (char)((i >> 8) & 0xFF);
                int d = (p + HashToIndex(span, dims)) % dims;
                word[d] += 0.75;
            }
        }

        private static void AddSkipGrams1(ReadOnlySpan<char> s, int dims, Vector<double> word)
        {
            int p = HashToIndex("sg1:".AsSpan(), dims);
            Span<char> span = stackalloc char[4];
            for (int i = 0; i + 2 < s.Length; i++)
            {
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
            Span<char> span = stackalloc char[3];
            if (s.Length >= 3)
            {
                span[0] = s[0]; span[1] = s[1]; span[2] = s[2];
                word[(pStart + HashToIndex(span, dims)) % dims] += 0.65;
                span[0] = s[s.Length - 3]; span[1] = s[s.Length - 2]; span[2] = s[s.Length - 1];
                word[(pEnd + HashToIndex(span, dims)) % dims] += 0.65;
            }
            else if (s.Length == 2)
            {
                span[0] = s[0]; span[1] = s[1]; span[2] = '_';
                word[(pStart + HashToIndex(span, dims)) % dims] += 0.65;
                span[0] = '_'; span[1] = s[0]; span[2] = s[1];
                word[(pEnd + HashToIndex(span, dims)) % dims] += 0.65;
            }
            else if (s.Length == 1)
            {
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

        // Determines if a token is a number using invariant culture, tolerant of leading/trailing spaces and sign.
        private static bool IsNumberToken(ReadOnlySpan<char> s)
        {
            if (s.IsEmpty) return false;

            // Trim spaces
            int start = 0, end = s.Length - 1;
            while (start <= end && char.IsWhiteSpace(s[start])) start++;
            while (end >= start && char.IsWhiteSpace(s[end])) end--;
            if (start > end) return false;

            s = s.Slice(start, end - start + 1);

            // Allow sign, decimal point, thousands separators and exponent (basic double pattern)
            // We avoid regex; rely on double.TryParse invariant culture.
            return double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out _);
        }

        // Detects longitude/latitude in common forms:
        // - Decimal degrees: "40.7128,-74.0060" or "40.7128 -74.0060"
        // - Single coordinate token like "40.7128N" or "74.0060W"
        // - Degrees/minutes/seconds (DMS) like "40°42'51\"N" or packed DMS like "404251°N" (40°42'51"N)
        // Returns true if the token itself represents lat or lon, or if the token contains both separated by comma/space.
        private static bool IsLongitudeLatitudeToken(ReadOnlySpan<char> s)
        {
            if (s.IsEmpty) return false;

            // Helper: parse decimal with optional trailing hemisphere letter
            static bool TryParseDecimalWithHemisphere(ReadOnlySpan<char> span, out double value, out char hemi)
            {
                hemi = '\0';
                value = 0.0;

                // Trim
                int start = 0, end = span.Length - 1;
                while (start <= end && char.IsWhiteSpace(span[start])) start++;
                while (end >= start && char.IsWhiteSpace(span[end])) end--;
                if (start > end) return false;
                span = span.Slice(start, end - start + 1);

                // Optional trailing hemisphere letter (N,S,E,W)
                char last = span[span.Length - 1];
                if (last is 'N' or 'n' or 'S' or 's' or 'E' or 'e' or 'W' or 'w')
                {
                    hemi = char.ToUpperInvariant(last);
                    span = span.Slice(0, span.Length - 1);
                    while (span.Length > 0 && char.IsWhiteSpace(span[span.Length - 1])) span = span.Slice(0, span.Length - 1);
                }

                if (!double.TryParse(span, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out value))
                    return false;

                return true;
            }

            // Validate lat range [-90, +90], lon range [-180, +180]
            static bool InLatRange(double v) => v >= -90.0 && v <= 90.0;
            static bool InLonRange(double v) => v >= -180.0 && v <= 180.0;

            // Case 1: coordinate pair like "lat,lon" or "lat lon"
            int commaIdx = s.IndexOf(',');
            if (commaIdx >= 0)
            {
                var left = s.Slice(0, commaIdx);
                var right = s.Slice(commaIdx + 1);
                if (TryParseDecimalWithHemisphere(left, out var lat, out var lh) &&
                    TryParseDecimalWithHemisphere(right, out var lon, out var rh))
                {
                    bool latOk = InLatRange(lat) && (lh is '\0' or 'N' or 'S');
                    bool lonOk = InLonRange(lon) && (rh is '\0' or 'E' or 'W');
                    if (latOk && lonOk) return true;
                }
            }
            else
            {
                int spaceIdx = s.IndexOf(' ');
                if (spaceIdx > 0)
                {
                    var left = s.Slice(0, spaceIdx);
                    var right = s.Slice(spaceIdx + 1);
                    if (TryParseDecimalWithHemisphere(left, out var lat, out var lh) &&
                        TryParseDecimalWithHemisphere(right, out var lon, out var rh))
                    {
                        bool latOk = InLatRange(lat) && (lh is '\0' or 'N' or 'S');
                        bool lonOk = InLonRange(lon) && (rh is '\0' or 'E' or 'W');
                        if (latOk && lonOk) return true;
                    }
                }
            }

            // Case 2: single coordinate with hemisphere, e.g., "40.7128N" or "74.0060W"
            if (TryParseDecimalWithHemisphere(s, out var single, out var hemi))
            {
                if (hemi is 'N' or 'S') return InLatRange(single);
                if (hemi is 'E' or 'W') return InLonRange(single);
                if (InLatRange(single) || InLonRange(single)) return true;
            }

            // Case 3a: standard DMS with delimiters (e.g., 40°42'51"N)
            int degIdx = s.IndexOf('°');
            if (degIdx > 0)
            {
                var degPart = s.Slice(0, degIdx);
                if (double.TryParse(degPart, NumberStyles.Float, CultureInfo.InvariantCulture, out var deg))
                {
                    bool likelyLat = deg >= 0 && deg <= 90;
                    bool likelyLon = deg >= 0 && deg <= 180;
                    bool hasMin = s.IndexOf('\'') > degIdx;
                    bool hasSec = s.IndexOf('\"') > degIdx;
                    if ((hasMin || hasSec) && (likelyLat || likelyLon))
                    {
                        char last = s[s.Length - 1];
                        if (last is 'N' or 'n' or 'S' or 's') return likelyLat;
                        if (last is 'E' or 'e' or 'W' or 'w') return likelyLon;
                        return true;
                    }
                }

                // Case 3b: packed DMS before '°', e.g., "404156°N" => 40°41'56"N
                // Accept 5-6 digits: DDMMSS or DDDMMSS (lon can have 3 digit degrees)
                // After '°' we may have hemisphere letter; minutes/seconds are implied by packing.
                var packed = s.Slice(0, degIdx);
                // Count digits
                int digitCount = 0;
                for (int i = 0; i < packed.Length; i++)
                {
                    if (char.IsDigit(packed[i])) digitCount++;
                    else return false; // non-digit found in packed segment
                }

                if (digitCount is 5 or 6 or 7) // 5/6 for lat, 6/7 for lon (e.g., 1234045 => 123°40'45")
                {
                    // Parse degrees/minutes/seconds by splitting from the end: SS (2), MM (2), rest degrees
                    int ss = 0, mm = 0, dd = 0;
                    // Extract integers without allocations
                    ReadOnlySpan<char> span = packed;
                    // seconds
                    if (!int.TryParse(span.Slice(span.Length - 2), NumberStyles.None, CultureInfo.InvariantCulture, out ss)) return false;
                    // minutes
                    if (!int.TryParse(span.Slice(span.Length - 4, 2), NumberStyles.None, CultureInfo.InvariantCulture, out mm)) return false;
                    // degrees
                    if (!int.TryParse(span.Slice(0, span.Length - 4), NumberStyles.None, CultureInfo.InvariantCulture, out dd)) return false;

                    // Validate minutes/seconds
                    if (mm < 0 || mm >= 60 || ss < 0 || ss >= 60) return false;

                    // Build decimal degrees
                    double decimalDegrees = dd + (mm / 60.0) + (ss / 3600.0);

                    // Hemisphere letter if any (last character)
                    char last = s[s.Length - 1];
                    bool hasHemi = last is 'N' or 'n' or 'S' or 's' or 'E' or 'e' or 'W' or 'w';

                    if (hasHemi)
                    {
                        last = char.ToUpperInvariant(last);
                        if (last is 'N' or 'S')
                        {
                            // Latitude: degrees must be within [0,90]
                            if (decimalDegrees <= 90.0) return true;
                            return false;
                        }
                        else
                        {
                            // Longitude: degrees must be within [0,180]
                            if (decimalDegrees <= 180.0) return true;
                            return false;
                        }
                    }
                    else
                    {
                        // No hemisphere: accept if value fits either range
                        if (decimalDegrees <= 90.0 || decimalDegrees <= 180.0) return true;
                    }
                }
            }

            return false;
        }

        // Adds a categorical feature to the vector that encodes:
        // 1) number / not-number
        // 2) lat/lon / not-lat/lon
        private void AddNumericAndGeoFeatures(ReadOnlySpan<char> labelSpan, Vector<double> word)
        {
            // Base feature groups
            int pNum = HashToIndex("feat:number:".AsSpan(), _numOfDimensions);
            int pGeo = HashToIndex("feat:geo:".AsSpan(), _numOfDimensions);

            bool isNumber = IsNumberToken(labelSpan);
            bool isGeo = IsLongitudeLatitudeToken(labelSpan);

            // Encode as single hashed buckets to keep sparsity and reproducibility
            int dNum = (pNum + HashToIndex((isNumber ? "is" : "not").AsSpan(), _numOfDimensions)) % _numOfDimensions;
            int dGeo = (pGeo + HashToIndex((isGeo ? "is" : "not").AsSpan(), _numOfDimensions)) % _numOfDimensions;

            // Small weights so they act as hints rather than dominating other features
            word[dNum] += 0.6;
            word[dGeo] += 0.6;
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

                // Numeric and geographic features
                AddNumericAndGeoFeatures(labelSpan, word);

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
            var angle = VectorOperations.CosAngle(tokens.First().vector, tokens.Last().vector);
            return angle;
        }

        public double CompareToUnitVector(string str1)
        {
            var tokens = Tokenize(new[] { str1 });
            var angle = VectorOperations.CosAngle(tokens.First().vector, _unitVector);
            return angle;
        }
    }
}
