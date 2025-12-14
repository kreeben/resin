namespace Resin.TextAnalysis
{
    // Synthesizes nonsensical labels by selecting features and solving for char sequences
    // that map to desired hashed dimensions. Uses deterministic seed.
    public sealed class TokenSynthesizer
    {
        private readonly int _dims;
        private readonly Random _rng;

        public TokenSynthesizer(int dims, int seed = 12345)
        {
            _dims = dims;
            _rng = new Random(seed);
        }

        // Simple FNV-like hash for span-based keys (copied logically from StringAnalyzer)
        private static int HashToIndex(ReadOnlySpan<char> key, int dimensions)
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            ulong h = offset;
            for (int i = 0; i < key.Length; i++)
            {
                h ^= key[i];
                h *= prime;
            }
            return (int)(h % (ulong)dimensions);
        }

        // Construct a label aiming to hit a set of target dimensions using boundary + bigram features.
        // This is heuristic and produces human-readable but nonsensical strings.
        public string SynthesizeLabelForDimensions(ReadOnlySpan<int> targetDims)
        {
            Span<char> buf = stackalloc char[32]; // limit to short labels
            int len = 0;

            // Choose first/last chars to try to hit some buckets
            char[] alphabet = "abcdefghijklmnopqrstuvwxyz".ToCharArray();
            char first = alphabet[_rng.Next(alphabet.Length)];
            char last = alphabet[_rng.Next(alphabet.Length)];

            buf[len++] = first;
            // Middle: try to align bigrams to hashed dims
            for (int i = 0; i < 6 && len < buf.Length - 1; i++)
            {
                char next = alphabet[_rng.Next(alphabet.Length)];
                // Check if bigram at position i contributes near some target dim
                Span<char> bigramKey = stackalloc char[4];
                bigramKey[0] = buf[len - 1];
                bigramKey[1] = next;
                bigramKey[2] = (char)(i & 0xFF);
                bigramKey[3] = (char)((i >> 8) & 0xFF);
                int d = HashToIndex(bigramKey, _dims);
                // Accept if it matches any target (heuristic)
                bool hit = false;
                for (int t = 0; t < targetDims.Length; t++)
                {
                    if (d == targetDims[t]) { hit = true; break; }
                }
                buf[len++] = next;
                if (hit && len > 10) break;
            }

            buf[len++] = last;
            return new string(buf.Slice(0, len));
        }

        // Produce N synthetic labels for validation
        public IEnumerable<string> Synthesize(int count)
        {
            for (int i = 0; i < count; i++)
            {
                // Random set of target dims
                int k = 4 + _rng.Next(4);
                int[] targets = new int[k];
                for (int j = 0; j < k; j++) targets[j] = _rng.Next(_dims);
                yield return SynthesizeLabelForDimensions(targets);
            }
        }
    }
}
