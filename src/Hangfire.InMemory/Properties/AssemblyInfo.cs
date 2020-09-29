using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyTitle("Hangfire.InMemory")]
[assembly: AssemblyDescription("In-memory job storage for Hangfire with transactional implementation.")]
[assembly: Guid("0111B3E0-EB76-439B-969C-5C029ED74C51")]
[assembly: InternalsVisibleTo("Hangfire.InMemory.Tests")]

// Allow the generation of mocks for internal types
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]