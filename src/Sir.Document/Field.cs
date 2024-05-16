using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir.Documents
{
    [DebuggerDisplay("{Name}")]
    public class Field
    {
        public long KeyId { get; set; }
        public long? DocumentId { get; set; }
        public string Name { get; }
        public object Value { get; set; }
        public IEnumerable<ISerializableVector> Tokens { get; }

        public Field(string name, object value, long keyId = -1, long? documentId = null, IEnumerable<ISerializableVector> tokens = null)
        {
            if (name is null) throw new ArgumentNullException(nameof(name));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Name = name;
            Value = value;
            KeyId = keyId;
            DocumentId = documentId;
            Tokens = tokens;
        }
    }
}