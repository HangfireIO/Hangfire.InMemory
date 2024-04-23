using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ConsoleSample
{
    public class HarnessHostedService : BackgroundService
    {
        private readonly IBackgroundJobClient _backgroundJobs;
        private readonly ILogger<HarnessHostedService> _logger;

        public HarnessHostedService(IBackgroundJobClient backgroundJobs, ILogger<HarnessHostedService> logger)
        {
            _backgroundJobs = backgroundJobs ?? throw new ArgumentNullException(nameof(backgroundJobs));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var sw = Stopwatch.StartNew();

            Parallel.For(0, 25_000, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },  i =>
            {
                _backgroundJobs.Enqueue("default" ,() => Empty());
                _backgroundJobs.Enqueue("critical", () => Empty());
            });

            _logger.LogInformation($"Enqueued in {sw.Elapsed}");
            return Task.CompletedTask;
        }

        public static void Empty()
        {
        }
    }
}