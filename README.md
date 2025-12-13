# Resin

Resin is a vector space index based search engine, a vector database and an anything key/value store. It powers efficient string processing, vector operations, and custom storage primitives designed for speed and simplicity.

## Highlights
- Fast key/value storage with page/column readers and writers
- Practical text analysis utilities for strings, bags of words/chars, and vectors
- Commandline tools for building and validating lexicons and comparing strings
- Clean, dependency light design that is easy to extend

## Key/Value Column Semantics

### ColumnWriter
- **TryPut(TKey key, ReadOnlySpan<byte> value)**
  - Inserts the key/value only if the key does not exist in the column-wide snapshot (previous pages included).
  - Returns `false` when the key already exists; otherwise writes to the current page.
  - Triggers page serialization when the page becomes full.

- **PutOrAppend(TKey key, ReadOnlySpan<byte> value)**
  - If the key exists anywhere in the column, no new key is stored. Instead, values are linked using a fixed-size node (`LinkedAddressNode`) written to the value stream.
  - Tail-appending order: the original value remains first, followed by each appended value in insertion order. Address entry for the key points to the list head when linking is active.
  - If the key does not exist in the column snapshot, operates at the page level (insert/append within the current page) and may serialize when full.

### ColumnReader
- **Get(TKey key)**
  - Returns the value for `key`. If the key’s address entry points to a linked-list head, returns the concatenated bytes of all linked values.
  - Returns `ReadOnlySpan<byte>.Empty` when the key does not exist.

- **GetMany(TKey key, out int count)**
  - Returns a concatenated `ReadOnlySpan<byte>` of all values linked for `key` and outputs the number of items via `count`.
  - When the key points to a single raw value, returns that value and `count = 1`. If the key does not exist, returns empty and `count = 0`.

### TKey Restrictions for ColumnWriter/ColumnReader
When working with `TKey`, please adhere to the following restrictions to ensure proper functionality:

- `TKey` must be a value type (`struct`) and implement both `IEquatable<TKey>` and `IComparable<TKey>`.
- Ordering and equality must be stable across sessions. The column-wide key snapshot uses `BinarySearch`/sorting, so `CompareTo` must define a strict total order consistent with `Equals`.
- Page-level storage operates on `long` keys. For primitive numeric keys:
  - `double` and `float` are stored via their IEEE bit representations.
  - `int` and `long` are stored directly.
  - Other `TKey` types are hashed via `GetHashCode()` to a `long` for page-level operations.
- **Recommendation:** Use numeric primitives (`double`, `float`, `int`, `long`) for deterministic ordering and lookup. If using a custom struct, ensure:
  - `Equals` and `CompareTo` are consistent and deterministic.
  - `GetHashCode()` is stable and evenly distributed; collisions affect page-level operations since non-primitive keys are hashed to `long`.
- Keys must be comparable across the entire column; duplicate detection relies on the column snapshot and `BinarySearch` over sorted keys.

### Column model and set operations
- Each column stores any given `TKey` at most once in its column-wide snapshot (duplicate keys are prevented by both `TryPut` and `PutOrAppend`). This makes columns effectively sets of keys, enabling set operations such as union, intersection, and joins across columns. Linked values (via `PutOrAppend`) attach additional data to the existing key without introducing duplicates.

## Project Structure
- `Resin.KeyValue` — Low level storage primitives (page, column, and byte array readers/writers; sessions)
- `Resin.TextAnalysis` — String analysis, vector operations, and text models
- `Resin.WikipediaCommandLine` — CLI utilities: lexicon build/validate and string compare
- `Resin.TextAnalysis.Tests` — Unit tests for text analysis components

## Getting Started
1. **Prerequisites**: Ensure you have the .NET 9 SDK installed on your machine.
2. **Clone the Repository**: 

   git clone https://github.com/kreeben/resin

3. **Build the Project**: 

   dotnet build

4. **Wikipedia Setup and Local Testing**

- [Detailed Instructions](https://github.com/kreeben/resin/blob/sortedlist/src/README.md)
- Or open the local file at `src/README.md` in your working copy.

## Usage
- Use `Resin.KeyValue` for fast on disk structures and efficient read/write sessions.
- Use `Resin.TextAnalysis` for `StringAnalyzer`, `VectorOperations`, and similarity tooling.
- Use `Sir.Strings` models for feature extraction from text.

## Contributing
Contributions are welcome! Please open an issue or pull request with clear motivation, tests when applicable, and concise changes.

## License
This project is licensed under the MIT License.

## Learn More
- **Issues**: [Report Issues](https://github.com/kreeben/resin/issues)


