using EventBus;
using OrderService.Models;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace OrderService.RabbitMQ;

public class OrderPublisherMultiQueue
{
    private readonly IRabbitMqConnection _factory;

    public OrderPublisherMultiQueue(IRabbitMqConnection factory) => _factory = factory;

    public async Task PublishAsync<T>(Message<T> message, CancellationToken ct = default) where T : class
    {
        await using var connection = await _factory.CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync();


        //int numberOfShards = 5;  
        string exchangeName = "sharded_order_exchange";
        //int totalWeight = 100;  

        //int shardWeight = totalWeight / numberOfShards;

        //await channel.ExchangeDeclareAsync(exchangeName, "x-consistent-hash", durable: true);

        //for (int i = 1; i <= numberOfShards; i++)
        //{
        //    string queueName = $"order_queue_{i:00}";

        //    await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

        //    await channel.QueueBindAsync(
        //        queue: queueName,
        //        exchange: exchangeName,
        //        routingKey: shardWeight.ToString(), // Binding key is the weight
        //        arguments: null);
        //}

        var json = JsonSerializer.Serialize(message.Payload);
        var body = Encoding.UTF8.GetBytes(json);

        string shardingKey = message.RoutingKey.ToString();

        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Headers = new Dictionary<string, object> { { "hash-header", shardingKey } }
        };  

        await channel.BasicPublishAsync(
             exchange: exchangeName,
             mandatory: true,
             routingKey: shardingKey,  
             basicProperties: props,
             body: body);
    } 
}
