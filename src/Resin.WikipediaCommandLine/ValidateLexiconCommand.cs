using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.KeyValue;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    /// <summary>
    /// validatelexicon --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true
    /// </summary>
    public class ValidateLexiconCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(args["dir"]);
            var dataSource = new WikipediaCirrussearchDataSource(args["source"]).GetData(new HashSet<string> { args["field"] });
            var take = int.Parse(args["take"]);

            using (var tx = new WriteSession(dir, "wikipedia".ToHash()))
            using (var readSession = new ReadSession(tx))
            {
                var analyzer = new StringAnalyzer();

                // Positive validation against produced lexicon using requested payload
                var payload = dataSource.First().values.Take(take);
                var positive = analyzer.ValidateLexicon(payload, readSession, logger);
                logger.LogInformation("Positive validation result: {Result}", positive);

                // Synthesized negative tokens: probe angle gaps and construct labels heuristically
                var inspector = new LexiconInspector(readSession);
                var candidates = inspector.SampleAngles(32);
                var missingAngles = inspector.FindMissingAngles(candidates).ToList();
                logger.LogInformation("Synthesized probe: sampled={Sampled}, missing={Missing}", 32, missingAngles.Count);

                var synthesizer = new TokenSynthesizer(dims: 512, seed: 12345);
                var synthetic = synthesizer.Synthesize(Math.Max(8, missingAngles.Count)).ToArray();
                var syntheticResult = analyzer.ValidateLexicon(synthetic, readSession, logger);
                logger.LogInformation("Synthetic negative validation result (should be false): {Result}", syntheticResult);
            }
        }
    }
}
