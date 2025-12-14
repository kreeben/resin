using Resin.KeyValue;
using System;
using System.Collections.Generic;

namespace Resin.TextAnalysis
{
    // Enumerates existing angles and probes for missing keys in the column.
    public sealed class LexiconInspector
    {
        private readonly ReadSession _readSession;
        private readonly ColumnReader<double> _reader;

        public LexiconInspector(ReadSession readSession)
        {
            _readSession = readSession ?? throw new ArgumentNullException(nameof(readSession));
            _reader = new ColumnReader<double>(_readSession);
        }

        // Probe a sequence of angle candidates, returning those not present.
        public IEnumerable<double> FindMissingAngles(IEnumerable<double> candidates)
        {
            foreach (var angle in candidates)
            {
                var buf = _reader.Get(angle);
                if (buf.IsEmpty)
                {
                    yield return angle;
                }
            }
        }

        // Generate uniform samples in [-1, 1] to probe gap buckets.
        public IEnumerable<double> SampleAngles(int count, int seed = 12345)
        {
            var rng = new Random(seed);
            for (int i = 0; i < count; i++)
            {
                // Uniform in [-1,1]
                double a = rng.NextDouble() * 2.0 - 1.0;
                yield return a;
            }
        }
    }
}
