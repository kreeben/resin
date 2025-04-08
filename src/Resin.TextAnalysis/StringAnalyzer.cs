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
        private readonly int _pageSize;
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

        public StringAnalyzer(int numOfDimensions = 512, int pageSize = 4096)
        {
            _numOfDimensions = numOfDimensions;
            _pageSize = pageSize;
            _unitVector = CreateVector.Sparse<double>(_numOfDimensions, (double)1);
            _label = new List<char>(_numOfDimensions);
        }

        public void Compose(IEnumerable<string> source, ReadSession readSession, WriteTransaction tx, bool labelVectors = true, ILogger? logger = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            using (var tokenWriter = new ColumnWriter<double>(new DoubleWriter(tx, _pageSize)))
            {
                var docCount = 0;
                long tokenCount = 0;
                foreach (var str in source)
                {
                    foreach (var token in TokenizeIntoDouble(str, labelVectors))
                    {
                        double angle = _unitVector.CosAngle(token.vector);
                        var buf = tokenReader.Get(angle);
                        if (buf.IsEmpty)
                        {
                            throw new InvalidOperationException($"could not find '{token.label}' at {angle}");
                        }
                        var storedVec = buf.ToArray().ToVectorDouble(_numOfDimensions);
                        double mutualAngle = storedVec.CosAngle(token.vector);
                        var composedVec = CreateVector.Sparse<double>(_numOfDimensions);
                        composedVec[0] = angle;
                        composedVec[1] = mutualAngle;
                        if (tokenWriter.TryPut(angle, composedVec.GetBytes(x => BitConverter.GetBytes(x))))
                            tokenCount++;
                    }
                    docCount++;
                    if (logger != null)
                    {
                        logger.LogInformation($"COMPOSE: doc {docCount}");
                        logger.LogInformation($"token count: {tokenCount}");
                    }
                }
            }
        }

        public void BuildFirstOrderLexicon(IEnumerable<string> source, WriteTransaction tx, ILogger? log = null)
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
                    foreach (var token in TokenizeIntoDouble(str))
                    {
                        var angle = _unitVector.CosAngle(token.vector);
                        var vectorBuf = token.vector.GetBytes(x => BitConverter.GetBytes(x));

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

        public bool ValidateLexicon(IEnumerable<string> source, ReadSession readSession, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            var docCount = 0;
            foreach (var str in source)
            {
                foreach (var token in TokenizeIntoDouble(str))
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

        public bool ValidateComposed(IEnumerable<string> source, ReadSession readSession, ReadSession readSessionComposed, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var tokenReader = new ColumnReader<double>(readSession, sizeof(double), _pageSize);
            var tokenReaderComposed = new ColumnReader<double>(readSessionComposed, sizeof(double), _pageSize);
            var docCount = 0;
            foreach (var str in source)
            {
                foreach (var token in TokenizeIntoDouble(str))
                {
                    double angle = _unitVector.CosAngle(token.vector);
                    var buf = tokenReader.Get(angle);

                    if (buf.IsEmpty)
                    {
                        throw new InvalidOperationException($"could not find '{token.label}' at {angle}");
                    }

                    var storedVec = buf.ToArray().ToVectorDouble(_numOfDimensions);
                    double mutualAngle = storedVec.CosAngle(token.vector);
                    var composedVec = CreateVector.Sparse<double>(_numOfDimensions);
                    composedVec[0] = angle;
                    composedVec[1] = mutualAngle;
                    var bufComposed = tokenReaderComposed.Get(angle);
                    var storedComposedVec = bufComposed.ToArray().ToVectorDouble(_numOfDimensions);
                    double mutualAngleComposed = storedComposedVec.CosAngle(composedVec);
                    if (mutualAngleComposed < 0.99)
                    {
                        throw new InvalidOperationException($"could not find composed '{token.label}' at {angle}");
                    }
                }
                docCount++;
                if (log != null)
                    log.LogInformation($"VALID: doc {docCount} content: {str.Substring(0, 25)}...{str.Substring(Math.Max(0, str.Length - 25), Math.Min(25, str.Length))}");
            }

            return true;
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
                foreach (var token in TokenizeIntoDouble(str))
                {
                    var angle = _unitVector.CosAngle(token.vector);
                    var buf = tokenReader.Get(angle);

                    if (buf.IsEmpty)
                    {
                        throw new InvalidOperationException($"could not find '{token.label}' at {angle}");
                    }

                    var storedToken = buf.ToArray().ToVectorDouble(_numOfDimensions);
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
            foreach (var token in TokenizeIntoFloat(source))
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

        public IEnumerable<(string label, Vector<double> vector)> TokenizeIntoDouble(string source, bool labelVectors = true)
        {
            int index = 0;
            var word = CreateVector.Sparse<double>(_numOfDimensions);
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
                        word = CreateVector.Sparse<double>(_numOfDimensions);
                        _label.Clear();
                        index = 0;
                    }
                }
            }
            if (((SparseVectorStorage<double>)word.Storage).Values.Length > 0)
            {
                if (labelVectors)
                    yield return (new string(_label.ToArray()), word);
                else
                    yield return (string.Empty, word);
            }
            _label.Clear();
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
