using System.Text.Unicode;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using Microsoft.Extensions.Logging;
using Resin.KeyValue;

namespace Resin.TextAnalysis
{
    public class StringAnalyzer
    {
        private DirectoryInfo _workingDirectory;
        const int _pageSize = 4096;
        const int _numOfDimensions = 512;
        ulong _collectionId = "wikipedia".ToHash();
        Vector<float> _unitVector = CreateVector.Sparse<float>(_numOfDimensions, (float)1);

        public StringAnalyzer()
        {
        }

        public StringAnalyzer(DirectoryInfo workingDirectory)
        {
            _workingDirectory = workingDirectory;
        }

        public void Validate(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var streamFactory = new StreamFactory(_workingDirectory);

            using (var tokenKeyStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Key))
            using (var tokenValueStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Value))
            using (var tokenAddressStream = streamFactory.CreateReadStream(_collectionId, FileExtensions.Address))

            using (var tokenReader = new DoublePageReader(tokenKeyStream, tokenValueStream, tokenAddressStream, _pageSize))
            {
                foreach (var token in Tokenize(source, _numOfDimensions))
                {
                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);
                    var score = tokenReader.IndexOf(angle);

                    if (score < 0)
                    {
                        var msg = $"could not find {token.label} at {angle}";
                        log.LogInformation(msg);
                        throw new InvalidOperationException(msg);
                    }

                    if (log != null)
                        log.LogInformation($"VALID: {token.label}");

                }
            }
        }

        public void Analyze(IEnumerable<string> source, ILogger? log = null)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            var words = new SortedList<double, (string label, Vector<float> vector)>();
            var streamFactory = new StreamFactory(_workingDirectory);

            streamFactory.Truncate();

            using (var keyStream = streamFactory.CreateReadWriteStream(_collectionId, FileExtensions.Key))
            using (var valueStream = streamFactory.CreateReadWriteStream(_collectionId, FileExtensions.Value))
            using (var addressStream = streamFactory.CreateReadWriteStream(_collectionId, FileExtensions.Address))
            using (var columnWriter = new ColumnWriter<double>(new DoubleWriter(keyStream, valueStream, addressStream, _pageSize)))
            {
                foreach (var token in Tokenize(source, _numOfDimensions))
                {
                    if (log != null)
                        log.LogInformation($"ANALYZED: {token.label}");

                    var angle = VectorOperations.CosAngle(_unitVector, token.vector);

                    //if (!words.TryAdd(angle, (token.label, token.vector)))
                    //{
                    //    var existingVector = words[angle].vector;
                    //    var a = VectorOperations.CosAngle(existingVector, token.vector);

                    //    if (a < 0.99)
                    //    {
                    //        if (bucketWriter.TryPut(angle, VectorOperations.GetBytes(token.vector)))
                    //        {
                    //            if (log != null)
                    //                log.LogInformation($"**** {words[angle].label} is dupe of {token.label}. both have angle {angle}. individual angle {a}");
                    //        }
                    //    }
                    //}
                    //else
                    //{
                    //    if (!tokenWriter.TryPut(angle, VectorOperations.GetBytes(token.vector)))
                    //    {
                    //        throw new InvalidOperationException("what in the world is going on with the tokens???");
                    //    }
                    //}
                    columnWriter.TryPut(angle, VectorOperations.GetBytes(token.vector));
                }
                //bucketWriter.Serialize();
                columnWriter.Serialize();
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
