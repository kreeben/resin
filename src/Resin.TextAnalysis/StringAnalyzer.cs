using System.Text.Unicode;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using Microsoft.Extensions.Logging;
using Resin.KeyValue;

namespace Resin.TextAnalysis
{
    public class StringAnalyzer
    {
        private readonly DirectoryInfo _workingDirectory;
        private readonly int _pageSize;
        private readonly int _numOfDimensions;
        private readonly ulong _collectionId;
        private readonly Vector<float> _unitVector;

        public StringAnalyzer(DirectoryInfo? workingDirectory = null)
        {
            _workingDirectory = workingDirectory;
            _numOfDimensions = 512;
            _pageSize = 4096;
            _unitVector = CreateVector.Sparse<float>(_numOfDimensions, (float)1);
            _collectionId = "wikipedia".ToHash();
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

        public void ValidateLexicon(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            foreach (var token in Tokenize(source))
            {
                var angle = _unitVector.CosAngle(token.vector);
                var buf = tokenReader.Get(angle);

                if (buf.IsEmpty)
                {
                    var msg = $"could not find '{token.label}' at {angle}";
                    log.LogError(new EventId(-1), msg);
                    throw new InvalidOperationException(msg);
                }

                var storedToken = buf.ToArray().ToVector(_numOfDimensions);
                var mutualAngle = storedToken.CosAngle(token.vector);
                if (mutualAngle < 0.99)
                {
                    var storedLabel = storedToken.AsString();
                    var msg = $"LEADER FOUND at angle:{angle}! query/label: {token.label}/{storedLabel} mutual angle: {mutualAngle}";
                    log.LogWarning(new EventId(-2), msg);
                }

                if (log != null)
                    log.LogInformation($"VALID: '{token.label}' angle: {angle}");

            }
        }

        public void BuildLexicon(IEnumerable<string> source, WriteTransaction tx, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx, _pageSize, batchMode: true)))
            {
                var tokens = Tokenize(source);
                foreach (var token in tokens)
                {
                    if (log != null)
                        log.LogInformation($"LEXICON: {token.label}");

                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);
                    var vectorBuf = VectorOperations.GetBytes(token.vector);

                    pageWriter.TryPut(angle, vectorBuf);
                }
                pageWriter.Serialize();
            }
        }

        public void Analyze(IEnumerable<(string key, IEnumerable<string> values)> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            //Parallel.ForEach(source, column =>
            //{
            //using (var tx = new WriteTransaction(_workingDirectory, _collectionId))
            //using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx, _pageSize)))
            //{
            //    foreach (var token in Tokenize(column.values, _numOfDimensions))
            //    {
            //        if (log != null)
            //            log.LogInformation($"ANALYZED: {token.label}");

            //        var angle = VectorOperations.CosAngle(_unitVector, token.vector);
            //        var vectorBuf = VectorOperations.GetBytes(token.vector);

            //        pageWriter.PutOrAppend(angle, vectorBuf);
            //    }
            //    pageWriter.Serialize();
            //}
            //});
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

        public IEnumerable<(string label, Vector<float> vector)> Tokenize(IEnumerable<string> source)
        {
            int count = 0;
            const char delimiter = ' ';
            foreach (var str in source)
            {
                int index = 0;
                var word = CreateVector.Sparse<float>(_numOfDimensions);
                var label = new List<char>();
                foreach (var c in str.ToCharArray())
                {
                    if (c == delimiter)
                    {
                        yield return (new string(label.ToArray()), word);
                        index = 0;
                        word = CreateVector.Sparse<float>(_numOfDimensions);
                        label.Clear();
                    }
                    else
                    {
                        word[index++] = c;
                        label.Add(c);
                    }
                }
                if (((SparseVectorStorage<float>)word.Storage).Values.Length > 0)
                {
                    yield return (new string(label.ToArray()), word);
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
