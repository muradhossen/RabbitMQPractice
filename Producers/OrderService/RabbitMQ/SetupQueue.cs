using EventBus;
using OrderService.Models;
using RabbitMQ.Client;

namespace OrderService.RabbitMQ;

public class SetupQueue
{
    private readonly IRabbitMqConnection _factory;

    public SetupQueue(IRabbitMqConnection factory) => _factory = factory;
    public async Task SetupMultiQueue(QueueConfig config, CancellationToken token = default)
    {
        var connection = await _factory.CreateConnectionAsync(token);
        var channel = await connection.CreateChannelAsync();
         
        await channel.ExchangeDeclareAsync(config.ExchangeName, "x-consistent-hash", durable: true, cancellationToken: token);
         
        int weight = 100 / config.NumberOfShards;

        for (int i = 1; i <= config.NumberOfShards; i++)
        {
            string queueName = $"{config.QueueName}_{i:00}";

            await channel.QueueDeclareAsync(queueName, true, false, false, cancellationToken: token);
            await channel.QueueBindAsync(queueName, config.ExchangeName, weight.ToString(), cancellationToken: token);
        }

        await channel.CloseAsync(token);
        await connection.CloseAsync(token);
    }

    public async Task SetupSingleQueue(QueueConfig config, CancellationToken token = default)
    {
        var connection = await _factory.CreateConnectionAsync(token);
        var channel = await connection.CreateChannelAsync();


        await channel.ExchangeDeclareAsync(config.ExchangeName, ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(config.QueueName, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(config.QueueName, config.ExchangeName, config.RoutingKey);

        await channel.CloseAsync(token);
        await connection.CloseAsync(token);

    }
}

public class QueueConfig
{
    public string QueueName { get; set; } = string.Empty;
    public string ExchangeName { get; set; } = string.Empty;
    public string RoutingKey { get; set; } = string.Empty;
    public int NumberOfShards { get; set; } = 1;
}