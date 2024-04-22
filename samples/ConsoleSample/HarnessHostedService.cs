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

            Parallel.For(0, 250_000, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },  i =>
            {
                _backgroundJobs.Enqueue(() => EmptyDefault());
                _backgroundJobs.Enqueue(() => EmptyCritical());
            });

            _logger.LogInformation($"Enqueued in {sw.Elapsed}");
            return Task.CompletedTask;
        }

        [Queue("default")]
        public static void EmptyDefault()
        {
        }

        [Queue("critical")]
        public static void EmptyCritical()
        {
        }
    }
}