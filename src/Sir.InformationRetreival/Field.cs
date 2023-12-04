using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    [DebuggerDisplay("{Name}")]
    public class Field
    {
        private IEnumerable<ISerializableVector> _tokens;

        public VectorNode Tree { get; private set; }
        public long KeyId { get; set; }
        public long DocumentId { get; set; }
        public string Name { get; }
        public object Value { get; set; }
        public IEnumerable<ISerializableVector> Tokens { get { return _tokens; } }

        public Field(string name, object value, long keyId = -1, long documentId = -1)
        {
            if (name is null) throw new ArgumentNullException(nameof(name));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Name = name;
            Value = value;
            KeyId = keyId;
            DocumentId = documentId;
        }
    }
}