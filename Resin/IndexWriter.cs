using System;
using System.Collections.Generic;
using System.IO;
using ProtoBuf;

namespace Resin
{
    public class IndexWriter : IDisposable
    {
        private readonly string _directory;
        private readonly IAnalyzer _analyzer;
        private readonly IDictionary<string, int> _fieldIndex; 
        private readonly IDictionary<int, FieldFile> _fieldFiles;
        private readonly string _fieldIndexFileName;
        private readonly DocumentFile _docFile;

        public IndexWriter(string directory, IAnalyzer analyzer)
        {
            _directory = directory;
            _analyzer = analyzer;

            _docFile = new DocumentFile(directory);
            _fieldFiles = new Dictionary<int, FieldFile>();
            _fieldIndexFileName = Path.Combine(_directory, "fld.ix");

            if (File.Exists(_fieldIndexFileName))
            {
                using (var fs = File.OpenRead(_fieldIndexFileName))
                {
                    _fieldIndex = Serializer.Deserialize<Dictionary<string, int>>(fs);
                }
            }
            else
            {
                _fieldIndex = new Dictionary<string, int>();
            }

            if (!Directory.Exists(directory)) Directory.CreateDirectory(directory);
        }

        public void Write(Document doc)
        {
            foreach (var field in doc.Fields)
            {
                int fieldId;
                if (!_fieldIndex.TryGetValue(field.Key, out fieldId))
                {
                    fieldId = GetNextFreeFieldId();
                    _fieldIndex.Add(field.Key, fieldId);
                }

                FieldFile ff;
                if (!_fieldFiles.TryGetValue(fieldId, out ff))
                {
                    var fileName = Path.Combine(_directory, fieldId + ".fld");
                    ff = new FieldFile(fileName);
                    _fieldFiles.Add(fieldId, ff);
                }
                foreach (var value in field.Value)
                {
                    _docFile.Write(doc.Id, field.Key, value);

                    var tokens = _analyzer.Analyze(value);
                    for (int position = 0; position < tokens.Length; position++)
                    {
                        ff.Write(doc.Id, tokens[position], position);
                    }
                }
            }
        }

        private int GetNextFreeFieldId()
        {
            return _fieldIndex.Count;
        }

        private void Flush()
        {
            _docFile.Dispose();

            foreach (var fieldFile in _fieldFiles)
            {
                fieldFile.Value.Dispose();
            }

            using (var fs = File.Create(_fieldIndexFileName))
            {
                Serializer.Serialize(fs, _fieldIndex);
            }
        }

        public void Dispose()
        {
            Flush();
        }
    }
}