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
        private readonly List<char> _label;
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

            // normalized ones vector
            var ones = CreateVector.Dense<double>(_numOfDimensions, 1.0);
            var norm = ones.L2Norm();
            _unitVector = ones / norm;

            // remove shared mutable label; handled locally in tokenizers
            _label = new List<char>(_numOfDimensions);
        }

        public void BuildLexicon(IEnumerable<string> source, WriteSession tx, ILogger? log = null)
        {
            if (source is null)
                throw new ArgumentNullException(nameof(source));

            using (var writer = new ColumnWriter<double>(new DoubleWriter(tx)))
            {
                long docCount = 0;
                long tokenCount = 0;

                foreach (var str in source)
                {
                    foreach (var token in TokenizeIntoDouble(str, labelVectors: true, useCharNGrams: true))
                    {
                        var key = StableKeyFromLabel(token.label);
                        var tokenBuf = token.vector.GetBytes(x => BitConverter.GetBytes(x));

                        if (writer.TryPut(key, tokenBuf))
                        {
                            tokenCount++;
                        }
                        else
                        {
                            Debug.WriteLine($"could not add token '{token.label}' at key {key}");
                        }
                    }

                    docCount++;
                    log?.LogInformation($"doc count: {docCount} tokens: {tokenCount}");
                }

                writer.Serialize();
            }
        }

        public bool ValidateLexicon(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
                throw new ArgumentNullException(nameof(source));

            var tokenReader = new ColumnReader<double>(readSession);
            var docCount = 0;

            foreach (var str in source)
            {
                foreach (var token in TokenizeIntoDouble(str, labelVectors: true, useCharNGrams: true))
                {
                    var key = StableKeyFromLabel(token.label);
                    var tokenBuf = tokenReader.Get(key);

                    if (tokenBuf.IsEmpty)
                        throw new InvalidOperationException($"could not find '{token.label}' at {key}");

                    var tokenVec = tokenBuf.ToVectorDouble(_numOfDimensions);
                    var mutualAngle = tokenVec.CosAngle(token.vector);

                    // Keep your corruption/collision signal
                    if (mutualAngle < 0.99)
                        throw new InvalidOperationException($"mutual angle of {mutualAngle} is too low. token: {token.label}");
                }

                docCount++;
                if (log != null)
                {
                    var headLen = Math.Min(25, str.Length);
                    var tailLen = Math.Min(25, str.Length);
                    var head = str.Substring(0, headLen);
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

        public IEnumerable<(string label, Vector<float> vector)> TokenizeIntoFloat(string source, bool labelVectors = true)
        {
            int index = 0;
            var word = CreateVector.Sparse<float>(_numOfDimensions);
            foreach (var c in source.ToCharArray())
            {
                if (IsData(c))
                {
                    if (index < _numOfDimensions)
                    {
                        word[index] = c;
                        _label.Add(c);
                        index++;
                    }
                }
                else if (c == '’')
                {
                    var cat = char.GetUnicodeCategory(c);
                }
                else
                {
                    if (index > 0)
                    {
                        if (labelVectors)
                            yield return (new string(_label.ToArray()), word);
                        else
                            yield return (string.Empty, word);
                        word = CreateVector.Sparse<float>(_numOfDimensions);
                        _label.Clear();
                        index = 0;
                    }
                }
            }
            if (((SparseVectorStorage<float>)word.Storage).Values.Length > 0)
            {
                if (labelVectors)
                    yield return (new string(_label.ToArray()), word);
                else
                    yield return (string.Empty, word);
            }
            _label.Clear();
        }

        // Add utility: deterministic n-gram extraction
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

        // Deterministic hashing to dimension index
        private static int HashToIndex(string key, int dims)
        {
            // 64-bit FNV-1a (simple, fast, deterministic)
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

        // Stateless character-based tokenization that composes a word vector from subword n-grams
        public IEnumerable<(string label, Vector<double> vector)> TokenizeIntoDouble(string source, bool labelVectors = true, bool useCharNGrams = true)
        {
            if (source == null)
                yield break;

            var labelBuffer = new List<char>(_numOfDimensions);
            var word = CreateVector.Sparse<double>(_numOfDimensions);
            int index = 0;

            foreach (var c in source)
            {
                if (IsData(c))
                {
                    if (index < _numOfDimensions)
                    {
                        // base character contribution
                        word[index] = c;
                        labelBuffer.Add(c);
                        index++;
                    }
                }
                else
                {
                    if (labelBuffer.Count > 0)
                    {
                        var label = new string(labelBuffer.ToArray());
                        if (useCharNGrams)
                        {
                            // add deterministic subword features
                            foreach (var ng in GetCharNGrams(label))
                            {
                                var dim = HashToIndex(ng, _numOfDimensions);
                                word[dim] += 1.0;
                            }
                        }
                        yield return (labelVectors ? label : string.Empty, word);
                        word = CreateVector.Sparse<double>(_numOfDimensions);
                        labelBuffer.Clear();
                        index = 0;
                    }
                }
            }

            if (((SparseVectorStorage<double>)word.Storage).Values.Length > 0 && labelBuffer.Count > 0)
            {
                var label = new string(labelBuffer.ToArray());
                if (useCharNGrams)
                {
                    foreach (var ng in GetCharNGrams(label))
                    {
                        var dim = HashToIndex(ng, _numOfDimensions);
                        word[dim] += 1.0;
                    }
                }
                yield return (labelVectors ? label : string.Empty, word);
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

        // Map a 64-bit hash to a stable double in [0,1]
        private static double StableKeyFromLabel(string label)
        {
            // 64-bit FNV-1a
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            ulong h = offset;
            foreach (var ch in label)
            {
                h ^= ch;
                h *= prime;
            }
            // scale to [0,1] avoiding endpoints
            const double denom = 18446744073709551615.0; // 2^64 - 1
            return (h + 0.5) / (denom + 1.0);
        }
    }
}
