namespace Resin.MachineLearning
{
    public class VectorNode
    {
        public VectorNode Left { get; set; }
        public VectorNode Right { get; set; }
        public Token Token { get; set; }

        public override string ToString()
        {
            return Token == null ? "*" : Token.Label == null ? Token.Vector.ToString() : Token.Label;
        }
    }
}
