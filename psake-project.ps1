Include "packages\Hangfire.Build.0.3.3\tools\psake-common.ps1"

Task Default -Depends Pack

Task Test -Depends Compile -Description "Run unit and integration tests." {
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

Task Sign -Depends Pack -Description "Sign artifacts." {
    $version = Get-PackageVersion

    Exec {
        Submit-SigningRequest -InputArtifactPath ".\build\Hangfire.InMemory.$version.nupkg" -OrganizationId $env:SIGNPATH_ORGANIZATION_ID -ApiToken $env:SIGNPATH_API_TOKEN -ProjectSlug "hangfire" -SigningPolicySlug "hangfire-test-signing-policy" -ArtifactConfigurationSlug "initial" -WaitForCompletion -OutputArtifactPath ".\build\Hangfire.InMemory.$version.signed.nupkg"         
    }
}
