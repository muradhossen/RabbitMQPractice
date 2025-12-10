using EventBus;
using RabbitMQ.Client;

namespace OrderService.RabbitMQ;

public class SetupQueue
{
    private readonly IRabbitMqConnection _factory;

    public SetupQueue(IRabbitMqConnection factory) => _factory = factory;
    public async Task SetupRabbitMqAsync(CancellationToken token)
    {
        var connection = await _factory.CreateConnectionAsync(token);
        var channel = await connection.CreateChannelAsync();

        string exchangeName = "sharded_order_exchange";
        await channel.ExchangeDeclareAsync(exchangeName, "x-consistent-hash", durable: true,cancellationToken: token);


        int numberOfShards = 5;
        int weight = 20;

        for (int i = 1; i <= numberOfShards; i++)
        {
            string queueName = $"order_queue_{i:00}";

            await channel.QueueDeclareAsync(queueName, true, false, false,cancellationToken: token);
            await channel.QueueBindAsync(queueName, exchangeName, weight.ToString(),cancellationToken: token);
        }

        await channel.CloseAsync(token);
        await connection.CloseAsync(token);
    }
}
