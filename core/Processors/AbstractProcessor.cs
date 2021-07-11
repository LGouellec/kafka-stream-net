using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        protected ILog log = null;
        protected string logPrefix = "";

        public ProcessorContext Context { get; protected set; }

        public string Name { get; set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes => Key is ISerDes<K> ? (ISerDes<K>)Key : null;

        public ISerDes<V> ValueSerDes => Value is ISerDes<V> ? (ISerDes<V>)Value : null;

        public ISerDes Key { get; internal set; } = null;

        public ISerDes Value { get; internal set; } = null;

        public IList<IProcessor> Next { get; private set; } = new List<IProcessor>();

        #region Ctor

        protected AbstractProcessor()
            : this(null)
        {

        }

        protected AbstractProcessor(string name)
            : this(name, null, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : this(name, keySerdes, valueSerdes, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            Key = keySerdes;
            Value = valueSerdes;
            StateStores = stateStores != null ? new List<string>(stateStores) : new List<string>();
            log = Logger.GetLogger(GetType());
        }

        #endregion

        public virtual void Close()
        {
            foreach (var n in Next)
            {
                n.Close();
            }
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value, long ts)
        {
            Context.ChangeTimestamp(ts);
            Forward<K1, V1>(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            log.Debug($"{logPrefix}Forward<{nameof(K1)},{nameof(V1)}> message with key {key} and value {value} to each next processor");
            Parallel.ForEach(Next.OfType<IProcessor<K1, V1>>(), p => p.Process(key, value));
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            var processors = Next.OfType<IProcessor<K1, V1>>()
                .Where(p => p.Name.Equals(name));

            Parallel.ForEach(processors, processor =>
            {
                log.Debug($"{logPrefix}Forward<{nameof(K1)},{nameof(V1)}> message with key {key} and value {value} to processor {name}");
                processor.Process(key, value);
            });
        }

        public virtual void Forward(K key, V value, string name)
        {
            var processors = Next.Where(p => p.Name.Equals(name));

            Parallel.ForEach(processors, processor =>
            {
                log.Debug($"{logPrefix}Forward<{nameof(K)},{nameof(V)}> message with key {key} and value {value} to processor {name}");
                if (processor is IProcessor<K, V> genericProcessor)
                    genericProcessor.Process(key, value);
                else
                    processor.Process(key, value);
            });
        }

        public virtual void Forward(K key, V value)
        {
            log.Debug($"{logPrefix}Forward<{nameof(K)},{nameof(V)}> message with key {key} and value {value} to each next processor");

            Parallel.ForEach(Next, processor =>
            {
                if (processor is IProcessor<K, V> genericProcessor)
                    genericProcessor.Process(key, value);
                else
                    processor.Process(key, value);
            });
        }

        #endregion

        public virtual void Init(ProcessorContext context)
        {
            log.Debug($"{logPrefix}Initializing process context");
            Context = context;
            foreach (var n in Next)
            {
                n.Init(context);
            }
            log.Debug($"{logPrefix}Process context initialized");
        }

        protected void LogProcessingKeyValue(K key, V value) => log.Debug($"{logPrefix}Process<{nameof(K)},{nameof(V)}> message with key {key} and {value} with record metadata [topic:{Context.RecordContext.Topic}|partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}]");

        #region Setter

        internal void SetTaskId(TaskId id)
        {
            logPrefix = $"stream-task[{id.Id}|{id.Partition}]|processor[{Name}]- ";
        }

        public void AddNextProcessor(IProcessor next)
        {
            if (next != null && !Next.Contains(next as IProcessor))
                Next.Add(next);
        }

        #endregion

        #region Process object

        public void Process(ConsumeResult<byte[], byte[]> record)
        {
            bool throwException = false;
            ObjectDeserialized key = null;
            ObjectDeserialized value = null;

            if (KeySerDes != null)
            {
                key = DeserializeKey(record);
                if (key.MustBeSkipped)
                {
                    log.Debug($"{logPrefix} Message with record metadata [topic:{Context.RecordContext.Topic}|partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}] was skipped !");
                    return;
                }
            }
            else
                throwException = true;

            if (ValueSerDes != null)
            {
                value = DeserializeValue(record);
                if (value.MustBeSkipped)
                {
                    log.Debug($"{logPrefix} Message with record metadata [topic:{Context.RecordContext.Topic}|partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}] was skipped !");
                    return;
                }
            }
            else
                throwException = true;

            if (throwException)
            {
                var s = KeySerDes == null ? "key" : "value";
                log.Error($"{logPrefix}Impossible to receive source data because keySerdes and/or valueSerdes is not setted ! KeySerdes : {(KeySerDes != null ? KeySerDes.GetType().Name : "NULL")} | ValueSerdes : {(ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL")}.");
                throw new StreamsException($"{logPrefix}The {s} serdes is not compatible to the actual {s} for this processor. Change the default {s} serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
            }
            else
                Process(key.Bean, value.Bean);
        }

        public void Process(object key, object value)
        {
            if((key == null || key is K) && (value == null || value is V))
                Process((K)key, (V)value);
        }

        #endregion

        #region Abstract

        public abstract void Process(K key, V value);

        #endregion

        public override bool Equals(object obj)
        {
            return obj is AbstractProcessor<K, V> && ((AbstractProcessor<K, V>)obj).Name.Equals(Name);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public virtual ObjectDeserialized DeserializeKey(ConsumeResult<byte[], byte[]> record)
        {
            try
            {
                var o = Key.DeserializeObject(record.Message.Key, new SerializationContext(MessageComponentType.Key, record.Topic, record.Message.Headers));
                return new ObjectDeserialized(o, false);
            }catch(Exception e)
            {
                var handlerResponse = Context.Configuration.DeserializationExceptionHandler != null ?
                    Context.Configuration.DeserializationExceptionHandler(Context, record, e) : ExceptionHandlerResponse.FAIL;

                if (handlerResponse == ExceptionHandlerResponse.FAIL)
                    throw new DeserializationException($"{ logPrefix }Error during key deserialization[Topic:{ record.Topic}| Partition:{ record.Partition}| Offset:{ record.Offset}| Timestamp:{ record.Message.Timestamp.UnixTimestampMs}]", e);
                else
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error during key deserialization [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}] with exception {e}.");
                    sb.AppendLine($"{logPrefix}DeserializationExceptionHandler return 'CONTINUE', so this message will be skipped and not processed !");
                    log.Error(sb.ToString());
                    return ObjectDeserialized.ObjectSkipped;
                }
            }

        }

        public virtual ObjectDeserialized DeserializeValue(ConsumeResult<byte[], byte[]> record)
        {
            try
            {
                var o = Value.DeserializeObject(record.Message.Value, new SerializationContext(MessageComponentType.Value, record.Topic, record.Message.Headers));
                return new ObjectDeserialized(o, false);
            }
            catch (Exception e)
            {
                var handlerResponse = Context.Configuration.DeserializationExceptionHandler != null ?
                    Context.Configuration.DeserializationExceptionHandler(Context, record, e) : ExceptionHandlerResponse.FAIL;

                if (handlerResponse == ExceptionHandlerResponse.FAIL)
                    throw new DeserializationException($"{ logPrefix }Error during value deserialization[Topic:{ record.Topic}| Partition:{ record.Partition}| Offset:{ record.Offset}| Timestamp:{ record.Message.Timestamp.UnixTimestampMs}]", e);
                else
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error during value deserialization [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}] with exception {e}.");
                    sb.AppendLine($"{logPrefix}DeserializationExceptionHandler return 'CONTINUE', so this message will be skipped and not processed !");
                    log.Error(sb.ToString());
                    return ObjectDeserialized.ObjectSkipped;
                }
            }
        }
    }
}
