using Hangfire.Storage;

namespace Hangfire.Memory
{
    internal class MemoryFetchedJob : IFetchedJob
    {
        private readonly IMemoryDispatcher _dispatcher;

        public MemoryFetchedJob(IMemoryDispatcher dispatcher, string queueName, string jobId)
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
            _dispatcher.QueryAndWait(state => state.QueueGetOrCreate(QueueName).Enqueue(JobId));
            _dispatcher.SignalOneQueueWaitNode();
        }
    }
}
