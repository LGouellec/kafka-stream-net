﻿using Confluent.Kafka;
using Kafka.Streams.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Kafka.Streams.Net.Mock.Pipes
{
    internal class PipeBuilder
    {
        private IKafkaSupplier kafkaSupplier;

        public PipeBuilder(IKafkaSupplier kafkaSupplier)
        {
            this.kafkaSupplier = kafkaSupplier;
        }

        internal IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new PipeInput(topic, configuration, kafkaSupplier);
        }

        internal IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new PipeOutput(topic, consumeTimeout, configuration, kafkaSupplier, token);
        }
    }
}