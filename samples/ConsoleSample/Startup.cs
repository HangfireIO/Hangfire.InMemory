using System;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace ConsoleSample
{
    public class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddHangfire(config => config
                .UseIgnoredAssemblyVersionTypeResolver()
                .UseInMemoryStorage());

            services.AddHostedService<HarnessHostedService>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseDeveloperExceptionPage();
            app.UseHangfireDashboard(String.Empty);

            app.UseHangfireServer();
        }
    }
}
