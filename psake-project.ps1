Framework 4.5.1
Include "packages\Hangfire.Build.0.2.6\tools\psake-common.ps1"

Task Default -Depends Collect

Task CompileCore -Depends Clean {
    Exec { dotnet build -c Release }
}

Task Test -Depends CompileCore -Description "Run unit and integration tests." {
    Exec { dotnet test --no-build -c Release "tests\Hangfire.InMemory.Tests" }
}

Task Collect -Depends Test -Description "Copy all artifacts to the build folder." {
    Collect-Assembly "Hangfire.InMemory" "net451"
    Collect-Assembly "Hangfire.InMemory" "netstandard2.0"
    Collect-File "LICENSE_ROYALTYFREE"
    Collect-File "LICENSE_STANDARD"
    Collect-File "COPYING.LESSER"
    Collect-File "COPYING"
    Collect-File "LICENSE.md"
}

Task Pack -Depends Collect -Description "Create NuGet packages and archive files." {
    $version = Get-PackageVersion
    
    Create-Archive "Hangfire.InMemory-$version"
    Create-Package "Hangfire.InMemory" $version
}
