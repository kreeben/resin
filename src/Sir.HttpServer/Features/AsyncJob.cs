namespace Sir.HttpServer.Features
{
    public abstract class AsyncJob : BaseJob
    {
        public string Id { get; }
        public string Job { get; }

        public AsyncJob(string id, string[] collection, string[] field, string q, string job, bool and, bool or)
            : base(collection, field, q, and, or)
        {
            Id = id;
            Job = job;
        }

        public override string ToString()
        {
            return $"{Collections} {Fields} {Job} {Id}";
        }
    }
}