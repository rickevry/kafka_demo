using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsole
{


    class Program
    {

        static int counter = 1;
        static string topic = "myTopic";
        static ProducerConfig producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

        static void Main(string[] args)
        {
            try
            {
                new Thread(() => Sender("Sender1")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                new Thread(() => Receiver("mygroup")).Start();
                Console.ReadLine();
            }
            catch (Exception e)
            {
                System.Console.WriteLine("Error", e.StackTrace);
            }
        }

        static async Task Send(int counter)
        {
            using (var p = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                try
                {
                    string msg = $"Welcome to Kafka {counter}";
                    // Console.WriteLine(msg);
                    var dr = await p.ProduceAsync(topic, new Message<Null, string> { Value = msg });
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

       static void Sender(string name)
        {
            while(true)
            {
                Send(counter++).Wait();
                Send(counter++).Wait();
                Send(counter++).Wait();
                Send(counter++).Wait();
                Thread.Sleep(1000);
            }
        }

        static void Receiver(string name)
        {

            ConsumerConfig consumerConfig = new ConsumerConfig
            {
                GroupId = $"{name}-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"{name} consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }

    }
}
