﻿using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors.Internal
{
    public class GlobalStateManagerTests
    {
        private GlobalStateManager stateManager;
        private Mock<IKeyValueStore<object, object>> kvStoreMock;
        private Mock<IKeyValueStore<object, object>> otherStoreMock;
        private Mock<IAdminClient> adminClientMock;
        private ProcessorTopology topology;

        private string kvStoreName = "kv-store";
        private string kvStoreTopic = "kv-store-topic";
        private string otherStoreName = "other-store";
        private string otherStoreTopic = "other-store-topic";

        [SetUp]
        public void SetUp()
        {
            kvStoreMock = CreateMockStore<IKeyValueStore<object, object>>(kvStoreName);
            otherStoreMock = CreateMockStore<IKeyValueStore<object, object>>(otherStoreName);
            var globalStateStores = new Dictionary<string, IStateStore>() { 
                { kvStoreMock.Object.Name, kvStoreMock.Object },
                { otherStoreMock.Object.Name, otherStoreMock.Object }
            };
            var storesToTopics = new Dictionary<string, string>() { 
                { kvStoreMock.Object.Name, kvStoreTopic },
                { otherStoreMock.Object.Name, otherStoreTopic }
            };

            this.topology = new ProcessorTopology(
                    null,
                    new Dictionary<string, IProcessor>(),
                    new Dictionary<string, IProcessor>(),
                    new Dictionary<string, IProcessor>(),
                    new Dictionary<string, IStateStore>(),
                    globalStateStores,
                    storesToTopics);

            adminClientMock = new Mock<IAdminClient>();
            RegisterPartitionInAdminClient(kvStoreTopic);
            RegisterPartitionInAdminClient(otherStoreTopic);

            stateManager = new GlobalStateManager(
                    topology,
                    adminClientMock.Object
                );
        }

        [Test]
        public void ShouldInitializeStateStores()
        {
            stateManager.Initialize();

            kvStoreMock.Verify(store => store.Init(It.IsAny<ProcessorContext>(), It.IsAny<IStateStore>()), Times.Once);
        }

        [Test]
        public void ShouldInitializeWithGlobalContext()
        {
            var processorContext = new ProcessorContext(null, null);
            stateManager.SetGlobalProcessorContext(processorContext);

            stateManager.Initialize();
            
            kvStoreMock.Verify(store => store.Init(processorContext, It.IsAny<IStateStore>()), Times.Once);
        }

        [Test]
        public void ShouldReturnInitializedStoreNames()
        {
            ISet<string> storeNames = stateManager.Initialize();

            Assert.AreEqual(new HashSet<string>() { kvStoreName, otherStoreName }, storeNames);
        }

        [Test]
        public void ShouldThrowIfSameStoreTwiceTwiceInTopology()
        {
            // this was already registered once
            this.topology.GlobalStateStores.Add($"kvStoreMock.Object.Name{1}", kvStoreMock.Object);

            Assert.Throws<ArgumentException>(() => stateManager.Initialize());
        }

        [Test]
        public void ShouldThrowIfNoPartitionsFoundForStore()
        {
            adminClientMock.Setup(client => client.GetMetadata(kvStoreTopic, It.IsAny<TimeSpan>())).Returns((Metadata)null);
            Assert.Throws<StreamsException>(() => stateManager.Initialize());
        }

        [Test]
        public void ShouldSetChangelogOffsetToZero()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);

            Assert.AreEqual(0, stateManager.ChangelogOffsets.Single(x => x.Key.Topic == kvStoreTopic).Value);
        }

        [Test]
        public void ShouldReturnStore()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);

            var result = stateManager.GetStore(kvStoreName);

            Assert.AreEqual(kvStoreMock.Object, result);
        }

        [Test]
        public void ShouldReturnNullIfNoStore()
        {
            stateManager.Initialize();

            var result = stateManager.GetStore("some-store-name");

            Assert.AreEqual(null, result);
        }

        [Test]
        public void ShouldFlushStateStores()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            stateManager.Register(otherStoreMock.Object, null);

            stateManager.Flush();

            kvStoreMock.Verify(x => x.Flush(), Times.Once);
            otherStoreMock.Verify(x => x.Flush(), Times.Once);
        }

        [Test]
        public void ShouldCloseStateStores()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            stateManager.Register(otherStoreMock.Object, null);

            stateManager.Close();

            kvStoreMock.Verify(x => x.Close(), Times.Once);
            otherStoreMock.Verify(x => x.Close(), Times.Once);
        }

        [Test]
        public void ShouldThrowIfStoreCloseFailed()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            kvStoreMock.Setup(x => x.Close()).Throws(new ProcessorStateException("boom!"));

            Assert.Throws<ProcessorStateException>(() => stateManager.Close());
        }

        [Test]
        public void ShouldAttemptToCloseAllStoresEvenWhenSomeException()
        {
            stateManager.Initialize();
            stateManager.Register(kvStoreMock.Object, null);
            stateManager.Register(otherStoreMock.Object, null);

            kvStoreMock.Setup(x => x.Close()).Throws(new ProcessorStateException("boom!"));

            try
            {
                stateManager.Close();
            }
            catch
            {
                // expected
            }

            kvStoreMock.Verify(x => x.Close(), Times.Once);
            otherStoreMock.Verify(x => x.Close(), Times.Once);
        }

        private Mock<T> CreateMockStore<T>(string name, bool isOpen = true) where T : class, IStateStore
        {
            var kvStore = new Mock<T>();
            kvStore.Setup(kvStore => kvStore.Name).Returns(name);
            kvStore.Setup(kvStore => kvStore.IsOpen).Returns(isOpen);

            return kvStore;
        }

        private void RegisterPartitionInAdminClient(string topic)
        {
            adminClientMock.Setup(client => client.GetMetadata(topic, It.IsAny<TimeSpan>())).Returns(
                new Metadata(
                    null,
                    new List<TopicMetadata>() {
                        new TopicMetadata(
                            topic,
                            new List<PartitionMetadata>() { new PartitionMetadata(0, 0, new int[] { }, new int[] { }, null) },
                            null)
                    }, 0, "")
            );
        }
    }
}