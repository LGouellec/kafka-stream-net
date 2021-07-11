using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.IntegrationTests.Fixtures;
using Streamiz.Kafka.Net.SerDes;
using Xunit;

namespace Streamiz.Kafka.Net.IntegrationTests
{
    public class IntegrationTest : IClassFixture<KafkaFixture>
    {
        private readonly KafkaFixture kafkaFixture;

        public IntegrationTest(KafkaFixture kafkaFixture)
        {
            this.kafkaFixture = kafkaFixture;
        }

        [Fact]
        public async Task TestSimpleTopology()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await kafkaFixture.CreateTopic("topic");
            await kafkaFixture.CreateTopic("topic2");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            await kafkaFixture.Produce(
                "topic", "", Encoding.UTF8.GetBytes("Hello world!")
            );
            
            await stream.StartAsync();

            var result = kafkaFixture.Consume("topic2");
            
            stream.Dispose();
            
            Assert.Equal("Hello world!", Encoding.UTF8.GetString(result.Message.Value));
        }
        
        [Fact]
        public async Task TestFilteredTopology()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await kafkaFixture.CreateTopic("filtered-topic");
            await kafkaFixture.CreateTopic("filtered-topic2");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("filtered-topic")
                .Filter((key, value) => value.StartsWith("a"))
                .To("filtered-topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            await kafkaFixture.Produce(
                "filtered-topic", "c", Encoding.UTF8.GetBytes("c Hello world!")
            );
            await kafkaFixture.Produce(
                "filtered-topic", "b", Encoding.UTF8.GetBytes("b Hello world!")
            );
            await kafkaFixture.Produce(
                "filtered-topic", "a", Encoding.UTF8.GetBytes("a Hello world!")
            );
            
            
            await stream.StartAsync();

            var result = kafkaFixture.Consume("filtered-topic2");
            
            stream.Dispose();
            
            Assert.Equal("a", result.Message.Key);
            Assert.Equal("a Hello world!", Encoding.UTF8.GetString(result.Message.Value));
        }
    }
}