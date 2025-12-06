using EventBus;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace OrderConsumer.RabbitMQ;

public class OrderConsumerService : BackgroundService
{
    private readonly RabbitMqConnection _factory;
    private IConnection? _connection;
    private IChannel? _channel;

    public OrderConsumerService(RabbitMqConnection factory) => _factory = factory;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _connection = await _factory.CreateConnectionAsync(cancellationToken);
        _channel = await _connection.CreateChannelAsync();

        // QoS
        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 100, global: false);

        // Ensure exchange/queue/binding exist (idempotent)
        await _channel.ExchangeDeclareAsync("order_exchange", ExchangeType.Direct, durable: true);
        await _channel.QueueDeclareAsync("order_queue", durable: true, exclusive: false, autoDelete: false);
        await _channel.QueueBindAsync("order_queue", "order_exchange", "order.created");

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var msg = Encoding.UTF8.GetString(ea.Body.ToArray());
            try
            {
                Console.WriteLine($"Received: {msg}");
                // Do real async work here
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);

                // ack
                await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Processing failed: {ex.Message}");
                // Nack and send to DLX depending on your strategy
                await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
            }
        };

        await _channel.BasicConsumeAsync(queue: "order_queue", autoAck: false, consumer: consumer);

        await base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel != null) { await _channel.CloseAsync(); await _channel.DisposeAsync(); }
        if (_connection != null) { await _connection.CloseAsync(); await _connection.DisposeAsync(); }
        await base.StopAsync(cancellationToken);
    }
}
