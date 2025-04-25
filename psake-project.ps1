Include "packages\Hangfire.Build.0.4.4\tools\psake-common.ps1"

Task Default -Depends Pack

Task Test -Depends Compile -Description "Run unit and integration tests." {
    Exec { dotnet test --no-build -c release "tests\Hangfire.InMemory.Tests" }
}

Task Collect -Depends Test -Description "Copy all artifacts to the build folder." {
    Collect-Assembly "Hangfire.InMemory" "net451"
    Collect-Assembly "Hangfire.InMemory" "netstandard2.0"
    Collect-Assembly "Hangfire.InMemory" "net6.0"
    Collect-File "LICENSE_ROYALTYFREE"
    Collect-File "LICENSE_STANDARD"
    Collect-File "COPYING.LESSER"
    Collect-File "COPYING"
    Collect-File "LICENSE.md"
    Collect-File "README.md"
}

Task Pack -Depends Collect -Description "Create NuGet packages and archive files." {
    $version = Get-PackageVersion

    Create-Package "Hangfire.InMemory" $version
    Create-Archive "Hangfire.InMemory-$version"
}

Task Sign -Depends Pack -Description "Sign artifacts." {
    $version = Get-PackageVersion

    Sign-ArchiveContents "Hangfire.InMemory-$version" "hangfire"
}
