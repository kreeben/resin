using System.Text.Unicode;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using Microsoft.Extensions.Logging;
using Resin.KeyValue;

namespace Resin.TextAnalysis
{
    public class StringAnalyzer
    {
        private readonly int _pageSize;
        private readonly int _numOfDimensions;
        private readonly Vector<float> _unitVector;

        public StringAnalyzer(int numOfDimensions = 512, int pageSize = 4096)
        {
            _numOfDimensions = numOfDimensions;
            _pageSize = pageSize;
            _unitVector = CreateVector.Sparse<float>(_numOfDimensions, (float)1);
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

        public void BuildLexicon(IEnumerable<string> source, WriteTransaction tx, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx, _pageSize, batchMode: true)))
            {
                long docCount = 0;
                long tokenCount = 0;
                foreach (var str in source)
                {
                    foreach (var token in Tokenize(str))
                    {
                        var angle = VectorOperations.CosAngle(_unitVector, token.vector);
                        var vectorBuf = VectorOperations.GetBytes(token.vector);

                        if (pageWriter.TryPut(angle, vectorBuf))
                        {
                            tokenCount++;
                        }
                    }
                    docCount++;
                    if (log != null)
                        log.LogInformation($"doc count: {docCount} tokens: {tokenCount}");
                }
                pageWriter.Serialize();
            }
        }

        public bool Validate(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            var docCount = 0;
            foreach (var str in source)
            {
                foreach (var token in Tokenize(str))
                {
                    var angle = _unitVector.CosAngle(token.vector);
                    var buf = tokenReader.Get(angle);

                    if (buf.IsEmpty)
                    {
                        throw new InvalidOperationException($"could not find '{token.label}' at {angle}");
                    }
                }
                docCount++;
                if (log != null)
                    log.LogInformation($"VALID: doc {docCount} content: {str.Substring(0, 25)}...{str.Substring(Math.Max(0, str.Length - 25), Math.Min(25, str.Length))}");
            }

            return true;
        }

        public void Analyze(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            var docCount = 0;
            foreach (var str in source)
            {
                foreach (var token in Tokenize(str))
                {
                    var angle = _unitVector.CosAngle(token.vector);
                    var buf = tokenReader.Get(angle);

                    if (buf.IsEmpty)
                    {
                        throw new InvalidOperationException($"could not find '{token.label}' at {angle}");
                    }

                    var storedToken = buf.ToArray().ToVector(_numOfDimensions);
                    var mutualAngle = storedToken.CosAngle(token.vector);
                    if (mutualAngle < 0.99)
                    {
                        var storedLabel = storedToken.AsString();
                        var msg = $"LEADER FOUND at angle:{angle}! query/label: {token.label}/{storedLabel} mutual angle: {mutualAngle}";
                        if (log != null)
                            log.LogInformation(msg);
                    }
                }
                docCount++;
                if (log != null)
                    log.LogInformation($"ANALYZE: doc {docCount}");
            }
        }

        public void FindClusters(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            var docCount = 0;
            foreach (var str in source)
            {
                foreach (var token in Tokenize(str))
                {
                    var angle = _unitVector.CosAngle(token.vector);
                    var buf = tokenReader.Get(angle);

                    if (buf.IsEmpty)
                    {
                        throw new InvalidOperationException($"could not find '{token.label}' at {angle}");
                    }

                    var storedToken = buf.ToArray().ToVector(_numOfDimensions);
                    var mutualAngle = storedToken.CosAngle(token.vector);
                    if (mutualAngle < 0.99)
                    {
                        var storedLabel = storedToken.AsString();
                        var msg = $"CLUSTER FOUND at angle:{angle}! query/label: {token.label}/{storedLabel} mutual angle: {mutualAngle}";
                        if (log != null)
                            log.LogInformation(msg);
                    }
                }
                docCount++;
                if (log != null)
                    log.LogInformation($"ANALYZE: doc {docCount}");
            }
        }

        public int FindMaxWordLength(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            int maxWordLen = 0;
            foreach (var token in Tokenize(source))
            {
                var lengtOfWord = ((SparseVectorStorage<float>)token.vector.Storage).Values.Length;
                if (lengtOfWord > maxWordLen)
                {
                    maxWordLen = lengtOfWord;
                }
            }
            if (log != null)
                log.LogInformation($"maxWordLen {maxWordLen}");
            return maxWordLen;
        }

        public IEnumerable<(string label, Vector<float> vector)> Tokenize(string source)
        {
            const char delimiter = ' ';
            int index = 0;
            var word = CreateVector.Sparse<float>(_numOfDimensions);
            var label = new List<char>();
            foreach (var c in source.ToCharArray())
            {
                if (c == delimiter || char.IsPunctuation(c))
                {
                    yield return (new string(label.ToArray()), word);
                    index = 0;
                    word = CreateVector.Sparse<float>(_numOfDimensions);
                    label.Clear();
                }
                else
                {
                    if (index < _numOfDimensions)
                    {
                        word[index] = c;
                        label.Add(c);
                    }
                    index++;
                }
            }
            if (((SparseVectorStorage<float>)word.Storage).Values.Length > 0)
            {
                yield return (new string(label.ToArray()), word);
            }
        }

        public IEnumerable<(string label, Vector<float> vector)> Tokenize(IEnumerable<string> source)
        {
            foreach (var str in source)
            {
                foreach (var token in Tokenize(str))
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
    }



}
