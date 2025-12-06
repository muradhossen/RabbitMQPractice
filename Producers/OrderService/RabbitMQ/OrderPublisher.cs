using EventBus;
using global::RabbitMQ.Client;
using System.Text;

namespace OrderService.RabbitMQ;

public class OrderPublisher
{
    private readonly RabbitMqConnection _factory;

    public OrderPublisher(RabbitMqConnection factory) => _factory = factory;

    public async Task PublishAsync(string message, CancellationToken ct = default)
    {
        // long-lived connection / channel recommended in production.
        // This example creates a connection+channel per publish for clarity.
        await using var connection = await _factory.CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync();

        // Async declarations (idempotent)
        await channel.ExchangeDeclareAsync("order_exchange", ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync("order_queue", durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(queue: "order_queue", exchange: "order_exchange", routingKey: "order.created");

        var body = Encoding.UTF8.GetBytes(message);

        // Create BasicProperties directly (CreateBasicProperties removed in v7)
        // BasicProperties type may live in RabbitMQ.Client.Framing or RabbitMQ.Client depending on package build.
        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent
        };

        // Async publish (throws on returned failure when confirms are enabled)
        await channel.BasicPublishAsync(
            exchange: "order_exchange",
            routingKey: "order.created",
            mandatory: false,
            basicProperties: props,
            body: body);
    }
}

