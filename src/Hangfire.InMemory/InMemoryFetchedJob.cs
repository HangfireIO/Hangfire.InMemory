using Hangfire.Storage;

namespace Hangfire.InMemory
{
    internal class InMemoryFetchedJob : IFetchedJob
    {
        private readonly InMemoryDispatcherBase _dispatcher;

        public InMemoryFetchedJob(InMemoryDispatcherBase dispatcher, string queueName, string jobId)
        {
            _dispatcher = dispatcher;

            QueueName = queueName;
            JobId = jobId;
        }

        public string QueueName { get; }
        public string JobId { get; }

        public void Dispose()
        {
        }

        public void RemoveFromQueue()
        {
        }

        public void Requeue()
        {
            var entry = _dispatcher.QueryAndWait(state =>
            {
                var value = state.QueueGetOrCreate(QueueName);
                value.Queue.Enqueue(JobId);
                return value;
            });

            _dispatcher.SignalOneQueueWaitNode(entry);
        }
    }
}
