﻿using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamThread : IThread
    {
        #region Static 

        public static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-producer";
        }

        public static string GetThreadProducerClientId(string threadClientId)
        {
            return threadClientId + "-producer";
        }

        public static string GetConsumerClientId(string threadClientId)
        {
            return threadClientId + "-consumer";
        }

        public static string GetRestoreConsumerClientId(string threadClientId)
        {
            return threadClientId + "-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static string GetSharedAdminClientId(string clientId)
        {
            return clientId + "-admin";
        }

        internal static IThread Create(string threadId, string clientId, InternalTopologyBuilder builder, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IAdminClient adminClient, int threadInd)
        {
            string logPrefix = $"stream-thread[{threadId}] ";
            var log = Logger.GetLogger(typeof(StreamThread));
            var customerID = $"{clientId}-StreamThread-{threadInd}";
            IProducer<byte[], byte[]> producer = null;

            // Due to limitations outlined in KIP-447 (which KIP-447 overcomes), it is
            // currently necessary to use a separate producer per input partition. The
            // producerState dictionary is used to keep track of these, and the current
            // consumed offset.
            // https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
            // IF Guarantee is AT_LEAST_ONCE, producer is the same of all StreamTasks in this thread, 
            // ELSE one producer by StreamTask.
            if (configuration.Guarantee == ProcessingGuarantee.AT_LEAST_ONCE)
            {
                log.Info($"{logPrefix}Creating shared producer client");
                producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig(GetThreadProducerClientId(threadId)));
            }

            var taskCreator = new TaskCreator(builder, configuration, threadId, kafkaSupplier, producer);
            var manager = new TaskManager(taskCreator, adminClient);

            var listener = new StreamsRebalanceListener(manager);

            log.Info($"{logPrefix}Creating consumer client");
            var consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig(customerID), listener);
            manager.Consumer = consumer;

            var thread = new StreamThread(threadId, customerID, manager, consumer, builder, TimeSpan.FromMilliseconds(configuration.PollMs), configuration.CommitIntervalMs);
            listener.Thread = thread;

            return thread;
        }

        #endregion

        public ThreadState State { get; private set; }

        private readonly ILog log = Logger.GetLogger(typeof(StreamThread));
        private readonly Thread thread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TaskManager manager;
        private readonly InternalTopologyBuilder builder;
        private readonly TimeSpan consumeTimeout;
        private readonly string threadId;
        private readonly string clientId;
        private readonly string logPrefix = "";
        private readonly long commitTimeMs = 0;
        private CancellationToken token;
        private DateTime lastCommit = DateTime.Now;

        private readonly object stateLock = new object();

        public event ThreadStateListener StateChanged;

        private StreamThread(string threadId, string clientId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, TimeSpan timeSpan, long commitInterval)
        {
            this.manager = manager;
            this.consumer = consumer;
            this.builder = builder;
            consumeTimeout = timeSpan;
            this.threadId = threadId;
            this.clientId = clientId;
            logPrefix = $"stream-thread[{threadId}] ";
            commitTimeMs = commitInterval;

            thread = new Thread(Run);
            thread.Name = this.threadId;

            State = ThreadState.CREATED;
        }

        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; } = false;

        public bool IsRunningAndNotRebalancing => State == ThreadState.RUNNING;

        public bool IsDisposable { get; private set; } = false;

        public int Id => thread.ManagedThreadId;

        public void Dispose() => Close(true);

        public void Run()
        {
            Exception exception = null;
            if (IsRunning)
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        if (exception != null)
                        {
                            Close(true);
                            throw exception;
                        }

                        ConsumeResult<byte[], byte[]> record = null;

                        if (State == ThreadState.PARTITIONS_ASSIGNED)
                        {
                            record = PollRequest(TimeSpan.Zero);
                        }
                        else if (State == ThreadState.PARTITIONS_REVOKED)
                        {
                            record = PollRequest(TimeSpan.Zero);
                        }
                        else if (State == ThreadState.RUNNING || State == ThreadState.STARTING)
                        {
                            record = PollRequest(consumeTimeout);
                        }
                        else
                        {
                            log.Error($"{logPrefix}Unexpected state {State} during normal iteration");
                            throw new StreamsException($"Unexpected state {State} during normal iteration");
                        }

                        if (record != null)
                        {
                            var task = manager.ActiveTaskFor(record.TopicPartition);
                            if (task != null)
                                task.AddRecords(record.TopicPartition, new List<ConsumeResult<byte[], byte[]>> { record });
                        }

                        foreach (var t in manager.ActiveTasks)
                        {
                            if (t.CanProcess)
                            {
                                bool b = t.Process();
                                if (b && t.CommitNeeded)
                                    t.Commit();
                            }
                        }

                        if (State == ThreadState.RUNNING)
                            MaybeCommit();

                        if (State == ThreadState.PARTITIONS_ASSIGNED)
                        {
                            SetState(ThreadState.RUNNING);
                        }
                    }
                    catch (KafkaException e)
                    {
                        log.Error($"{logPrefix}Encountered the following unexpected Kafka exception during processing, tis usually indicate Streams internal errors:", e);
                        exception = e;
                    }
                    catch (Exception e)
                    {
                        log.Error($"{logPrefix}Encountered the following error during processing:", e);
                        exception = e;
                    }
                }

                while (IsRunning)
                {
                    // Use for waiting end of disposing
                    Thread.Sleep(100);
                }
                
                // Dispose consumer
                try
                {
                    consumer.Dispose();
                }
                catch (Exception e)
                {
                    log.Error($"{logPrefix}Failed to close consumer due to the following error:", e);
                }
            }
        }

        public void Start(CancellationToken token)
        {
            log.Info($"{logPrefix}Starting");
            if (SetState(ThreadState.STARTING) == null)
            {
                log.Info($"{logPrefix}StreamThread already shutdown. Not running");
                IsRunning = false;
                return;
            }

            this.token = token;
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            thread.Start();
        }

        public IEnumerable<ITask> Tasks => this.manager.ActiveTasks;

        #endregion

        private bool MaybeCommit()
        {
            int committed = 0;
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(commitTimeMs))
            {
                DateTime beginCommit = DateTime.Now;
                log.Debug($"Committing all active tasks {string.Join(",", manager.ActiveTaskIds)} since {(DateTime.Now - lastCommit).TotalMilliseconds}ms has elapsed (commit interval is {commitTimeMs}ms)");
                committed = manager.CommitAll();
                if(committed > 0)
                    log.Debug($"Committed all active tasks {string.Join(",", manager.ActiveTaskIds)} in {(DateTime.Now - beginCommit).TotalMilliseconds}ms");

                if (committed == -1)
                {
                    log.Debug("Unable to commit as we are in the middle of a rebalance, will try again when it completes.");
                }
                else
                {
                    lastCommit = DateTime.Now;
                }
            }
            return committed > 0;
        }

        private void Close(bool cleanUp = true)
        {
            try
            {
                if (!IsDisposable)
                {
                    log.Info($"{logPrefix}Shutting down");

                    SetState(ThreadState.PENDING_SHUTDOWN);

                    manager.Close();
                    consumer.Unsubscribe();
                    IsRunning = false;
                    thread.Join();
                    
                    SetState(ThreadState.DEAD);
                    log.Info($"{logPrefix}Shutdown complete");
                    IsDisposable = true;
                }
            }
            catch (Exception e)
            {
                log.Error($"{logPrefix}Failed to close stream thread due to the following error:", e);
            }
        }

        private ConsumeResult<byte[], byte[]> PollRequest(TimeSpan ts)
        {
            return consumer.Consume(ts);
        }

        internal ThreadState SetState(ThreadState newState)
        {
            ThreadState oldState;

            lock (stateLock)
            {
                oldState = State;

                if (State == ThreadState.PENDING_SHUTDOWN && newState != ThreadState.DEAD)
                {
                    log.Debug($"{logPrefix}Ignoring request to transit from PENDING_SHUTDOWN to {newState}: only DEAD state is a valid next state");
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (State == ThreadState.DEAD)
                {
                    log.Debug($"{logPrefix}Ignoring request to transit from DEAD to {newState}: no valid next state after DEAD");
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (!State.IsValidTransition(newState))
                {
                    string logPrefix = "";
                    log.Error($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    log.Info($"{logPrefix}State transition from {oldState} to {newState}");
                }

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);

            return oldState;
        }
    }
}
