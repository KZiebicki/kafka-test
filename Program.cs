using System;
using Confluent.Kafka;
using Newtonsoft.Json;

class Program
{
    public static void Main(string[] args)
    {
        int runMode = 0;
        using (StreamReader reader = new StreamReader("config.json"))
        {
            string json = reader.ReadToEnd();
            runMode = ((dynamic)JsonConvert.DeserializeObject(json)).runMode;
        }

        switch (runMode)
        {
            case 1:
                KafkaProducer.RunProducer();
                break;
            case 2:
                KafkaConsumer.RunConsumer();
                break;
            case 3:
                Thread producerThread = new Thread(KafkaProducer.RunProducer);
                producerThread.Start();
                Thread consumerThread = new Thread(KafkaConsumer.RunConsumer);
                consumerThread.Start();
                break;
            default:
                Console.WriteLine("Wrong config!");
                break;
        }
        
    }
}

class KafkaProducer
{
    public static void RunProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092" // Kafka broker address
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            Console.WriteLine("[PRODUCER] Enter messages to send to Kafka (type 'exit' to quit):");

            string message;
            while ((message = Console.ReadLine()) != "exit")
            {
                try
                {
                    // Send message to Kafka topic
                    var deliveryResult = producer.ProduceAsync("test-topic", new Message<Null, string> { Value = message }).Result;
                    Console.WriteLine($"[PRODUCER] Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"[PRODUCER] Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}

class KafkaConsumer
{
    public static void RunConsumer()
    {
        var config = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("test-topic");
            Console.WriteLine("[CONSUMER] Listening for messages on 'test-topic'...");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"[CONSUMER] Received message: {consumeResult.Message.Value}");
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"[CONSUMER] Error occurred: {e.Error.Reason}");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
