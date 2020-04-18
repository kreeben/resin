using System.Collections.Generic;

namespace Sir.HttpServer.Features
{
    public abstract class AsyncJob : BaseJob
    {
        public string Id { get; }
        public string Job { get; }
        public IDictionary<string, object> Status { get; }

        public AsyncJob(string id, string[] collection, string[] field, string q, string job, bool and, bool or)
            : base(collection, field, q, and, or)
        {
            Id = id;
            Job = job;
            Status = new Dictionary<string, object>();
        }

        public override string ToString()
        {
            return $"{Collections} {Fields} {Job} {Id}";
        }
    }
}