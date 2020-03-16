namespace Sir.HttpServer.Features
{
    public abstract class AsyncJob
    {
        public abstract void Execute();

        public string Id { get; }
        public string[] Collection { get; }
        public string[] Field { get; }
        public string Q { get; }
        public string Job { get; }
        public bool And { get; }
        public bool Or { get; }

        public AsyncJob(string id, string[] collection, string[] field, string q, string job, bool and, bool or)
        {
            Id = id;
            Collection = collection;
            Field = field;
            Q = q;
            Job = job;
            And = and;
            Or = or;
        }


        public override string ToString()
        {
            return $"{Collection} {Field} {Job} {Id}";
        }
    }
}