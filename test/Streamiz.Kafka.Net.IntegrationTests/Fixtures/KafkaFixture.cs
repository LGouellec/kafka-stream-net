using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Confluent.Kafka;
using DotNet.Testcontainers.Containers.Builders;
using DotNet.Testcontainers.Containers.Configurations.MessageBrokers;
using DotNet.Testcontainers.Containers.Modules.MessageBrokers;
using Xunit;

namespace Streamiz.Kafka.Net.IntegrationTests.Fixtures
{
    public class KafkaFixture : IAsyncLifetime
    {
        private readonly KafkaTestcontainer container;
        
        public KafkaFixture()
        {
            container = new TestcontainersBuilder<KafkaTestcontainer>()
                .WithKafka(new KafkaTestcontainerConfiguration())
                .WithImage("public.ecr.aws/knowre/cp-kafka:6.1.0")
                .WithPortBinding(9092)
                .WithName("kafka-streamiz-integration-tests")
                .Build();
        }

        public string BootstrapServers => container.BootstrapServers;

        public ReadOnlyDictionary<string, string> ConsumerProperties => new(
            new Dictionary<string, string>
            {
                {"bootstrap.servers", container.BootstrapServers},
                {"auto.offset.reset", "earliest"},
                {"group.id", "sample-consumer"}
            }
        );

        public ProducerConfig ProducerProperties => new()
        {
            BootstrapServers = container.BootstrapServers
        };

        private IConsumer<string, byte[]> Consumer()
        {
            var consumer = new ConsumerBuilder<string, byte[]>(ConsumerProperties).Build();
            return consumer;
        }

        internal ConsumeResult<string, byte[]> Consume(string topic)
        {
            using var consumer = Consumer();
            consumer.Subscribe(topic);
            return consumer.Consume(TimeSpan.FromSeconds(5));
        }

        internal async Task<DeliveryResult<string, byte[]>> Produce(string topic, string key, byte[] bytes)
        {
            using var producer = new ProducerBuilder<string, byte[]>(ProducerProperties).Build();
            return await producer.ProduceAsync(topic, new Message<string, byte[]>()
            {
                Key = key,
                Value = bytes
            });
        }

        public async Task CreateTopic(string name, int partitions = 1)
        {
            await container.ExecAsync(new List<string>() {
                "/bin/sh",
                "-c",
                $"/usr/bin/kafka-topics --create --zookeeper {container.IpAddress}:2181 " +
                    "--replication-factor 1 " +
                    $"--partitions {partitions} " +
                    $"--topic {name}"
            });
        }
        
        public async Task DisposeAsync() => await container.DisposeAsync().AsTask();
        
        public async Task InitializeAsync() => await container.StartAsync();
        
    }
}