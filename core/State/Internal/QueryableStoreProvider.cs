﻿using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// A wrapper over all of the <see cref="IStateStoreProvider{T}"/>s in a Topology
    /// </summary>
    internal class QueryableStoreProvider
    {
        // TODO: uncomment GlobalStateStoreProvider code when it is available

        private readonly IEnumerable<StreamThreadStateStoreProvider> storeProviders;
        //private readonly GlobalStateStoreProvider globalStateStoreProvider;

        public QueryableStoreProvider(IEnumerable<StreamThreadStateStoreProvider> storeProviders
            /*GlobalStateStoreProvider globalStateStoreProvider*/)
        {
            this.storeProviders = new List<StreamThreadStateStoreProvider>(storeProviders);
            //this.globalStateStoreProvider = globalStateStoreProvider;
        }

        /// <summary>
        /// Get a composite object wrapping the instances of the <see cref="Processors.IStateStore"/> with the provided
        /// storeName and <see cref="IQueryableStoreType{T}"/>
        /// </summary>
        /// <typeparam name="T">The expected type of the returned store</typeparam>
        /// <param name="storeQueryParameters">parameters to be used when querying for store</param>
        /// <returns>A composite object that wraps the store instances.</returns>
        public T GetStore<T>(StoreQueryParameters<T> storeQueryParameters) where T : class
        {
            //IEnumerable<T> globalStore = this.globalStateStoreProvider.stores(storeName, queryableStoreType);
            //if (globalStore.Any())
            //{
            //    return queryableStoreType.Create(new WrappingStoreProvider(new[] { this.globalStateStoreProvider })), storeName);
            //}

            IEnumerable<T> allStores = this.storeProviders
                .SelectMany(store => store.Stores(storeQueryParameters));

            if (!allStores.Any())
            {
                throw new InvalidStateStoreException($"The state store, {storeQueryParameters.StoreName}, may have migrated to another instance.");
            }

            return storeQueryParameters
                .QueryableStoreType
                .Create(
                    new WrappingStoreProvider<T>(this.storeProviders, storeQueryParameters),
                    storeQueryParameters.StoreName);
        }
    }
}
