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
        private readonly int _pageSize = 4096;
        private readonly int _numOfDimensions;
        private readonly ulong _collectionId;
        private readonly Vector<float> _unitVector;

        public StringAnalyzer(DirectoryInfo? workingDirectory = null)
        {
            _workingDirectory = workingDirectory;
            _numOfDimensions = UnicodeRanges.All.Length;
            _unitVector = CreateVector.Sparse<float>(_numOfDimensions, (float)1);
            _collectionId = "wikipedia".ToHash();
        }

        public double Compare(string str1, string str2)
        {
            var tokens = Tokenize(new[] { str1, str2 }, _numOfDimensions);
            var angle = VectorOperations.CosAngle(tokens.First().vector, tokens.Last().vector);
            return angle;
        }

        public double CompareToUnitVector(string str1)
        {
            var tokens = Tokenize(new[] { str1 }, _numOfDimensions);
            var angle = VectorOperations.CosAngle(tokens.First().vector, _unitVector);
            return angle;
        }

        public void Validate(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            using (var session = new ReadSession(_workingDirectory, _collectionId))
            {
                var tokenReader = new ColumnReader<double>(session, sizeof(double), _pageSize);
                foreach (var token in Tokenize(source, _numOfDimensions))
                {
                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);
                    var buf = tokenReader.Get(angle);

                    if (buf.IsEmpty)
                    {
                        var msg = $"could not find '{token.label}' at {angle}";
                        log.LogInformation(msg);
                        throw new InvalidOperationException(msg);
                    }

                    //if (angle < 0.99)
                    //{
                    //    var msg = $"score {angle} is too low. label: {token.label}, angle:{angle}";
                    //    log.LogInformation(msg);
                    //    throw new InvalidOperationException(msg);
                    //}

                    if (log != null)
                        log.LogInformation($"VALID: '{token.label}' angle: {angle}");

                }
            }
        }

        public void Analyze(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            using (var tx = new WriteTransaction(_workingDirectory, _collectionId))
            using (var pageWriter = new ColumnWriter<double>(new DoubleWriter(tx, _pageSize)))
            {
                foreach (var token in Tokenize(source, _numOfDimensions))
                {
                    if (log != null)
                        log.LogInformation($"ANALYZED: {token.label}");

                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);
                    var vectorBuf = VectorOperations.GetBytes(token.vector);

                    pageWriter.PutOrAppend(angle, vectorBuf);
                }
                pageWriter.Serialize();
            }
        }

        public int FindMaxWordLength(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            const int numOfDimensions = 512;
            int maxWordLen = 0;
            foreach (var token in Tokenize(source, numOfDimensions))
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

        private IEnumerable<(Vector<float> vector, string label)> Tokenize(IEnumerable<string> source, int numOfDimensions)
        {
            int count = 0;
            const char delimiter = ' ';
            foreach (var str in source)
            {
                int index = 0;
                var word = CreateVector.Sparse<float>(numOfDimensions);
                var label = new List<char>();
                foreach (var c in str.ToCharArray())
                {
                    if (c == delimiter)
                    {
                        yield return (word, new string(label.ToArray()));
                        index = 0;
                        word = CreateVector.Sparse<float>(numOfDimensions);
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
                    yield return (word, new string(label.ToArray()));
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
