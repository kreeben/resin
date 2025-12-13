# Resin

Resin is a vector space index based search engine, a vector database and an anything key/value store. It powers efficient string processing, vector operations, and custom storage primitives designed for speed and simplicity.

## Highlights
- Fast key/value storage with page/column readers and writers
- Practical text analysis utilities for strings, bags of words/chars, and vectors
- Commandline tools for building and validating lexicons and comparing strings
- Clean, dependency light design that is easy to extend

## Project Structure
- `Resin.KeyValue` — Low level storage primitives (page, column, and byte array readers/writers; sessions)
- `Resin.TextAnalysis` — String analysis, vector operations, and text models
- `Sir.Strings` — BagofWords and BagofChars models
- `Resin.WikipediaCommandLine` — CLI utilities: lexicon build/validate and string compare
- `Resin.TextAnalysis.Tests` — Unit tests for text analysis components

## Getting Started
1. **Prerequisites**: Ensure you have the .NET 9 SDK installed on your machine.
2. **Clone the Repository**: 

   git clone https://github.com/kreeben/resin

3. **Build the Project**: 

   dotnet build

4. **Run CLI Examples**:
- Build lexicon: 
  ```bash
  dotnet run --project Resin.WikipediaCommandLine -- lexicon
  ```
- Validate lexicon: 
  ```bash
  dotnet run --project Resin.WikipediaCommandLine -- validate
  ```
- Compare strings: 
  ```bash
  dotnet run --project Resin.WikipediaCommandLine -- compare "left" "right"
  ```

## Usage
- Use `Resin.KeyValue` for fast on disk structures and efficient read/write sessions.
- Use `Resin.TextAnalysis` for `StringAnalyzer`, `VectorOperations`, and similarity tooling.
- Use `Sir.Strings` models for feature extraction from text.

 ## Wikipedia Setup and Local Testing

Step-by-step guide to download a Wikipedia CirrusSearch dump and run the build/validate commands: 

See the detailed instructions in `src/README.md`:
- [Detailed Instructions](https://github.com/kreeben/resin/blob/sortedlist/src/README.md)
- Or open the local file at `src/README.md` in your working copy.

This link provides the “Setup Wikipedia locally” flow, including the `wikipediadownload` command and usage examples for `lexicon` and `validatelexicon`.

## Contributing
Contributions are welcome! Please open an issue or pull request with clear motivation, tests when applicable, and concise changes.

## License
This project is licensed under the MIT License.

## Learn More
- **Issues**: [Report Issues](https://github.com/kreeben/resin/issues)


