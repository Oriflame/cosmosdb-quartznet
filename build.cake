#tool nuget:?package=NUnit.ConsoleRunner&version=3.16.3
//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument("target", "Default");
var configuration = Argument("configuration", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Clean")
    .Does(() =>
{
    CleanDirectory("./artifacts/");
});

Task("Restore")
    .IsDependentOn("Clean")
    .Does(() =>
{
    DotNetCoreRestore();
});

Task("Build")
    .IsDependentOn("Restore")
    .Does(() =>
{
    var settings = new DotNetCoreMSBuildSettings();
    settings.SetConfiguration(configuration);
    DotNetCoreMSBuild(settings);
});

Task("Test")
    .IsDependentOn("Build")
    .Does(() =>
{
    var settings = new DotNetCoreTestSettings
    {
        Configuration = "Release",
        Framework = "netcoreapp2.0"
    };

    DotNetCoreTest("./test/Quartz.Spi.CosmosDbJobStore.Tests/Quartz.Spi.CosmosDbJobStore.Tests.csproj", settings);
});

Task("Pack")
    .IsDependentOn("Build")
    .Does(() => 
{
    var settings = new DotNetCorePackSettings
    {
        Configuration = configuration,
        OutputDirectory = "./artifacts/"
    };

    DotNetCorePack("./src/Quartz.Spi.CosmosDbJobStore/Quartz.Spi.CosmosDbJobStore.csproj", settings);
});

Task("AppVeyor")
    .IsDependentOn("Test")
    .IsDependentOn("Pack")
    .Does(() => 
{

});

//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task("Default")
    .IsDependentOn("Build");

//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget(target);
