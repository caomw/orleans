using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using UnitTests.GrainInterfaces;
using UnitTests.Grains;
using UnitTests.Tester;
using Xunit;
using Xunit.Abstractions;

namespace Tester
{
    public class PinnedGrainTests : OrleansTestingBase, IClassFixture<PinnedGrainTestFixture>
    {
        private int NumSilos { get; }
        private readonly IManagementGrain mgmtGrain;
        private readonly ITestOutputHelper output;

        public PinnedGrainTests(ITestOutputHelper output, PinnedGrainTestFixture fixture)
        {
            this.output = output;
            TestCluster cluster = fixture.HostedCluster;
            NumSilos = cluster.GetActiveSilos().Count();
            mgmtGrain = GrainFactory.GetGrain<IManagementGrain>(RuntimeInterfaceConstants.SYSTEM_MANAGEMENT_ID);
            TestSilosStarted(PinnedGrainTestFixture.NumSilos);
        }

        [Fact, TestCategory("BVT"), TestCategory("PinnedGrains"), TestCategory("Placement")]
        public async Task Init_CheckEnv_PinnedGrainTests()
        {
            // This tests just bootstraps the 2 default test silos, and checks that partition grains were created on each.
            NumSilos.Should().Be(PinnedGrainTestFixture.NumSilos, "Should have expected number of silos");
            
            IPartitionManager partitionManager = GrainFactory.GetGrain<IPartitionManager>(0);
            IList<PartitionInfo> partitionInfos = await partitionManager.GetPartitionInfos();

            partitionInfos.Count.Should().Be(PinnedGrainTestFixture.NumSilos, "Should have results for expected number of silos silos");
            partitionInfos.Count.Should().Be(NumSilos, " PartitionInfo list should return {0} values.", NumSilos);
            partitionInfos[0].PartitionId.Should().NotBe(partitionInfos[1].PartitionId, "PartitionIds should be different.");
            await CountActivations("Initial");
        }

        [Fact, TestCategory("BVT"), TestCategory("PinnedGrains"), TestCategory("Placement")]
        public async Task SendMsg_Client_PinnedGrains()
        {
            IPartitionManager partitionManager = GrainFactory.GetGrain<IPartitionManager>(0);
            IList<PartitionInfo> partitionInfosList1 = await partitionManager.GetPartitionInfos();

            partitionInfosList1.Count.Should().Be(NumSilos, "Initial: PartitionInfo list should return {0} values.", NumSilos);
            partitionInfosList1[0].PartitionId.Should().NotBe(partitionInfosList1[1].PartitionId, "Initial: PartitionIds should be different.");
            await CountActivations("Initial");

            foreach (PartitionInfo partition in partitionInfosList1)
            {
                Guid partitionId = partition.PartitionId;
                IPartitionGrain grain = GrainFactory.GetGrain<IPartitionGrain>(partitionId);
                PartitionInfo pi = await grain.GetPartitionInfo();
                output.WriteLine(pi);
            }

            await CountActivations("After Send");

            IList<PartitionInfo> partitionInfosList2 = await partitionManager.GetPartitionInfos();

            partitionInfosList2.Count.Should().Be(NumSilos, "After Send: PartitionInfo list should return {0} values.", NumSilos);
            foreach (int i in Enumerable.Range(0, PinnedGrainTestFixture.NumSilos))
            {
                partitionInfosList1[i].PartitionId.Should().Be(partitionInfosList2[i].PartitionId, "After Send: Same PartitionIds [{0}]", i);
            }
            partitionInfosList2[0].PartitionId.Should().NotBe(partitionInfosList2[1].PartitionId, "After Send: PartitionIds should be different.");
            await CountActivations("After checks");
        }

        [Fact, TestCategory("BVT"), TestCategory("PinnedGrains"), TestCategory("Placement")]
        public async Task SendMsg_Broadcast_PinnedGrains()
        {
            IPartitionManager partitionManager = GrainFactory.GetGrain<IPartitionManager>(0);
            IList<PartitionInfo> partitionInfosList1 = await partitionManager.GetPartitionInfos();

            partitionInfosList1.Count.Should().Be(NumSilos, "Initial: PartitionInfo list should return {0} values.", NumSilos);
            partitionInfosList1[0].PartitionId.Should().NotBe(partitionInfosList1[1].PartitionId, "Initial: PartitionIds should be different.");
            await CountActivations("Initial");

            await partitionManager.Broadcast(p => p.GetPartitionInfo());

            await CountActivations("After Broadcast");

            IList<PartitionInfo> partitionInfosList2 = await partitionManager.GetPartitionInfos();

            partitionInfosList2.Count.Should().Be(NumSilos, "After Send: PartitionInfo list should return {0} values.", NumSilos);
            foreach (int i in Enumerable.Range(0, PinnedGrainTestFixture.NumSilos))
            {
                partitionInfosList1[i].PartitionId.Should().Be(partitionInfosList2[i].PartitionId, "After Send: Same PartitionIds [{0}]", i);
            }
            partitionInfosList2[0].PartitionId.Should().NotBe(partitionInfosList2[1].PartitionId, "After Send: PartitionIds should be different.");

            await CountActivations("After checks");
        }

        private async Task CountActivations(string when)
        {
            string grainType = typeof(PartitionGrain).FullName;
            int siloCount = PinnedGrainTestFixture.NumSilos;
            int expectedGrainsPerSilo = 1;
            IList<SimpleGrainStatistic> grainStats = (await mgmtGrain.GetSimpleGrainStatistics()).ToList();
            output.WriteLine("Got All Grain Stats: " + string.Join(" ", grainStats));
            IList<SimpleGrainStatistic> partitionGrains = grainStats.Where(gs => gs.GrainType == grainType).ToList();
            output.WriteLine("Got PartitionGrain Stats: " + string.Join(" ", partitionGrains));
            IList<SimpleGrainStatistic> wrongSilos = partitionGrains.Where(gs => gs.ActivationCount != expectedGrainsPerSilo).ToList();
            wrongSilos.Count.Should().Be(0, when + ": Silos with wrong number of {0} grains: {1}",
                grainType, string.Join(" ", wrongSilos));
            int count = partitionGrains.Select(gs => gs.ActivationCount).Sum();
            count.Should().Be(siloCount, when + ": Total count of {0} grains should be {1}. Got: {2}",
                grainType, siloCount, string.Join(" ", grainStats));
        }
    }

    public class PinnedGrain_SiloFailureTests : OrleansTestingBase, IDisposable
    {
        private int NumSilos { get; }
        private readonly TestCluster cluster;
        private readonly PinnedGrainTestFixture fixture;

        private readonly ITestOutputHelper output;

        public PinnedGrain_SiloFailureTests(ITestOutputHelper output)
        {
            this.output = output;

            fixture = new PinnedGrainTestFixture();
            cluster = fixture.HostedCluster;

            NumSilos = cluster.GetActiveSilos().Count();
            TestSilosStarted(PinnedGrainTestFixture.NumSilos);

            NodeConfiguration cfg = cluster.Primary.NodeConfiguration;
            output.WriteLine(
                "Primary silo: Address = {0} Proxy gateway: {1}", 
                cfg.Endpoint, cfg.ProxyGatewayEndpoint);
            foreach (int i in Enumerable.Range(0, cluster.SecondarySilos.Count))
            {
                cfg = cluster.SecondarySilos[i].NodeConfiguration;
                output.WriteLine(
                    "Secondary silo {0} : Address = {1} Proxy gateway: {2}",
                    i + 1, cfg.Endpoint, cfg.ProxyGatewayEndpoint);
            }
        }

        public virtual void Dispose()
        {
            fixture.Dispose();
        }

        [Theory]
        [InlineData("Primary", "Kill")]
        [InlineData("Primary", "Stop")]
        [InlineData("Secondary", "Kill")]
        [InlineData("Secondary", "Stop")]
        [TestCategory("BVT"), TestCategory("PinnedGrains"), TestCategory("Placement")]
        public async Task PinnedGrains_SiloFails(string siloToFail, string killOrStop)
        {
            IPartitionManager partitionManager = GrainFactory.GetGrain<IPartitionManager>(0);
            IList<PartitionInfo> partitionInfosList = await partitionManager.GetPartitionInfos();
            partitionInfosList.Count.Should().Be(NumSilos, "Initial: PartitionInfo list should return {0} values.", NumSilos);
            partitionInfosList[0].PartitionId.Should().NotBe(partitionInfosList[1].PartitionId, "Initial: PartitionIds should be different.");

            foreach (int i in Enumerable.Range(0, PinnedGrainTestFixture.NumSilos))
            {
                PartitionInfo pi = partitionInfosList[i];
                Guid partitionId = pi.PartitionId;
                output.WriteLine("Partition {0} is online on active silo {1}",
                    partitionId, pi.SiloId);
            }

            output.WriteLine("Silo to {0} = {1}", killOrStop, siloToFail);
            SiloHandle silo;
            if (siloToFail == "Primary")
            {
                silo = cluster.Primary;
            }
            else
            {
                silo = cluster.SecondarySilos.First();
            }
            SiloAddress deadSilo = silo.Silo.SiloAddress;
            output.WriteLine("About to {0} {1} silo {2}", killOrStop, siloToFail, deadSilo);
            if (killOrStop == "Kill")
            {
                cluster.KillSilo(silo); // Insta-death
            }
            else
            {
                cluster.StopSilo(silo); // Semi-gracefull stop
            }

            foreach (int i in Enumerable.Range(0, PinnedGrainTestFixture.NumSilos))
            {
                PartitionInfo pi = partitionInfosList[i];

                await CheckPartition(pi, deadSilo);
            }
        }

        private async Task CheckPartition(PartitionInfo pi, SiloAddress deadSilo)
        {
            string deadSiloId = deadSilo.ToString();

            Guid partitionId = pi.PartitionId;

            if (pi.SiloId.Equals(deadSiloId))
            {
                string msg = string.Format("Checking: Partition {0} should be offline on dead silo {1}",
                    partitionId, pi.SiloId);
                output.WriteLine(msg);

                await Assert.ThrowsAsync<SiloUnavailableException>(async () =>
                {
                    IPartitionGrain grain = GrainFactory.GetGrain<IPartitionGrain>(partitionId);
                    PartitionInfo partInfo = await grain.GetPartitionInfo();
                    output.WriteLine("Result: Found partition {0} on silo {1} but expected {2}",
                        partitionId, partInfo.SiloId, pi.SiloId);

                    partInfo.PartitionId.Should().Be(partitionId, "PartitionId should match.");
                    partInfo.SiloId.Should().Be(pi.SiloId, 
                        "Oops - pinned grain {0} has moved to another silo!", partitionId);
                });

                output.WriteLine(
                    "Got expected error talking to Partition {0} should be offline on dead silo {1}",
                    pi.PartitionId, pi.SiloId);
            }
            else
            {
                output.WriteLine("Checking: Partition {0} should be online on alive silo {1}",
                    partitionId, pi.SiloId);

                try
                {
                    IPartitionGrain grain = GrainFactory.GetGrain<IPartitionGrain>(partitionId);
                    PartitionInfo partInfo = await grain.GetPartitionInfo();
                    output.WriteLine("Result: Found partition {0} on silo {1}",
                        partitionId, partInfo.SiloId);

                    partInfo.PartitionId.Should().Be(partitionId, "PartitionId should match.");
                    partInfo.SiloId.Should().Be(pi.SiloId,
                        "Oops - pinned grain {0} has moved to another silo!", partitionId);
                }
                catch (Exception exc)
                {
                    output.WriteLine(
                        "Got un-expected error talking to Partition {0} should be online on alive silo {1}"
                        + Environment.NewLine + "Error = {2}",
                        pi.PartitionId, pi.SiloId, exc);

                    throw;
                }
            }
        }
    }

    #region PinnedGrain TestFixture
    public class PinnedGrainTestFixture : BaseTestClusterFixture
    {
        internal const int NumSilos = 2;

        protected override TestCluster CreateTestCluster()
        {
            TestClusterOptions options = new TestClusterOptions(NumSilos);

            // Note: Using MemoryStore for testing only.
            options.ClusterConfiguration.AddMemoryStorageProvider("PartitionManagerStore");
            options.ClusterConfiguration.Globals.RegisterBootstrapProvider(
                providerTypeFullName: typeof(PartitionStartup).FullName,
                providerName: "PartitionGrainStartup");
            options.ClusterConfiguration.Defaults.PropagateActivityId = true;

            options.ClientConfiguration.PropagateActivityId = true;

            return new TestCluster(options);
        }
    }
    #endregion
}
