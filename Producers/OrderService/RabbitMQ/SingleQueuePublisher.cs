using EventBus;
using global::RabbitMQ.Client;
using OrderService.Models;
using System.Text;
using System.Text.Json;

namespace OrderService.RabbitMQ;

public class SingleQueuePublisher
{
    private readonly IRabbitMqConnection _factory;

    public SingleQueuePublisher(IRabbitMqConnection factory) => _factory = factory;

    public async Task PublishAsync<T>(Message<T> message, CancellationToken ct = default) where T : class
    { 
        await using var connection = await _factory.CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync(); 

        var json = JsonSerializer.Serialize(message.Payload);
        var body = Encoding.UTF8.GetBytes(json); 
        
        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent
        };

        
        await channel.BasicPublishAsync(
            exchange: message.Exchange,
            routingKey: message.RoutingKey.ToString(),
            mandatory: false,
            basicProperties: props,
            body: body);
    }
}

