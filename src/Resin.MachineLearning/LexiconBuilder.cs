namespace Resin.MachineLearning
{
    public class LexiconBuilder
    {
        public IDictionary<string, int> BuildLexicon(IEnumerable<string> words)
        {
            var lexicon = new SortedList<string, int>();
            int index = 0;
            foreach (var word in words)
            {
                if (!lexicon.ContainsKey(word))
                {
                    lexicon.Add(word, index++);
                }
            }
            return lexicon;
        }
    }
}
