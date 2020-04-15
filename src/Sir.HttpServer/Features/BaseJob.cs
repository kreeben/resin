namespace Sir.HttpServer.Features
{
    public abstract class BaseJob
    {
        public abstract void Execute();

        public string[] Collections { get; }
        public string[] Fields { get; }
        public string Q { get; }
        public bool And { get; }
        public bool Or { get; }

        public BaseJob(string[] collections, string[] fields, string q, bool and, bool or)
        {
            Collections = collections;
            Fields = fields;
            Q = q;
            And = and;
            Or = or;
        }

        public override string ToString()
        {
            return $"{Collections} {Fields}";
        }
    }
}