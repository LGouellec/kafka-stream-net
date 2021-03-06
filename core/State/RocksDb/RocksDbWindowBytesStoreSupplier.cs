﻿using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.RocksDb.Internal;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbWindowBytesStoreSupplier : IWindowBytesStoreSupplier
    {
        private readonly TimeSpan retention;
        private readonly long segmentInterval;

        public RocksDbWindowBytesStoreSupplier(
            string storeName,
            TimeSpan retention,
            long segmentInterval,
            long? size)
        {
            Name = storeName;
            this.retention = retention;
            this.segmentInterval = segmentInterval;
            WindowSize = size;
        }

        public long? WindowSize { get; set; }

        public long Retention => (long)retention.TotalMilliseconds;

        public string Name { get; }

        public IWindowStore<Bytes, byte[]> Get()
        {
            return new RocksDbWindowStore(
                new RocksDbSegmentedBytesStore(
                    Name,
                    Retention,
                    segmentInterval,
                    new RocksDbWindowKeySchema()),
                WindowSize.HasValue ? WindowSize.Value : (long)TimeSpan.FromMinutes(1).TotalMilliseconds);
        }
    }
}
