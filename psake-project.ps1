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

    Create-Package "Hangfire.InMemory" $version
    Compress-Archive -Path "$build_dir/*" -DestinationPath "$build_dir/Hangfire.InMemory-$version.zip"
}

Task Sign -Depends Pack -Description "Sign artifacts." {
    $version = Get-PackageVersion

    Sign-ArchiveContents "Hangfire.InMemory-$version" "hangfire" "nuget-and-assemblies-in-zip-file"
}

function Sign-ArchiveContents($name, $project, $configuration) {
    $policy = "test-signing-policy"

    if ($env:APPVEYOR_REPO_TAG -eq "True") {
        $policy = "release-signing-policy"
    }

    Write-Host "Using signing project '$project'..." -ForegroundColor "DarkGray"
    Write-Host "Using signing policy '$policy'..." -ForegroundColor "DarkGray"
    Write-Host "Using artifacts configuration '$configuration'..." -ForegroundColor "DarkGray"

    $archive = "$build_dir/$name.zip"

    Write-Host "Submitting archive '$archive' for signing..." -ForegroundColor "Green"
    Submit-SigningRequest -InputArtifactPath "$archive" -OrganizationId $env:SIGNPATH_ORGANIZATION_ID -ApiToken $env:SIGNPATH_API_TOKEN -ProjectSlug "$project" -SigningPolicySlug "$policy" -ArtifactConfigurationSlug "$configuration" -WaitForCompletion -OutputArtifactPath "$archive" -Force

    Write-Host "Unpacking signed files..." -ForegroundColor "Green"
    Expand-Archive -Path "$archive" -DestinationPath "$build_dir" -Force -PassThru
}