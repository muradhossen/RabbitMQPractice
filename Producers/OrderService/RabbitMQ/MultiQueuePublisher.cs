using EventBus;
using OrderService.Models;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace OrderService.RabbitMQ;

public class MultiQueuePublisher
{
    private readonly IRabbitMqConnection _factory;

    public MultiQueuePublisher(IRabbitMqConnection factory) => _factory = factory;

    public async Task PublishAsync<T>(Message<T> message,CancellationToken ct = default) where T : class
    {
        await using var connection = await _factory.CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync();  

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
             exchange: message.Exchange,
             mandatory: true,
             routingKey: shardingKey,  
             basicProperties: props,
             body: body);
    } 
}
