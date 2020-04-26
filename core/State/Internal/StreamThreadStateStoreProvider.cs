﻿using Streamiz.Kafka.Net.Processors;
using System.Linq;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class StreamThreadStateStoreProvider
    {
        private readonly IThread streamThread;
        private readonly InternalTopologyBuilder internalTopologyBuilder;

        public StreamThreadStateStoreProvider(IThread streamThread, InternalTopologyBuilder internalTopologyBuilder)
        {
            this.streamThread = streamThread;
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        public IEnumerable<T> Stores<T>(StoreQueryParameters<T> storeQueryParameters) where T: class
        {
            // TODO: handle 'staleStoresEnabled' and 'partition' when they are added to StoreQueryParameters

            if (this.streamThread.State == ThreadState.DEAD)
            {
                return Enumerable.Empty<T>();
            }
            if (!(this.streamThread.State == ThreadState.RUNNING))
            {
                throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} because " +
                    $"the stream thread is {streamThread.State}, not RUNNING");
            }

            List<T> stores = new List<T>();
            foreach (var streamTask in streamThread.ActiveTasks)
            {
                IStateStore store = streamTask.GetStore(storeQueryParameters.StoreName);
                if (store != null && storeQueryParameters.QueryableStoreType.Accepts(store))
                {
                    if (!store.IsOpen)
                    {
                        throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} for task {streamTask} because the " +
                            $"store is not open. The state store may have migrated to another instances.");
                    }

                    // TODO: handle TimestampedKeyValueStore and TimestampedWindowStore 

                    stores.Add(store as T);
                }
            }
            return stores;
        }
    }
}
