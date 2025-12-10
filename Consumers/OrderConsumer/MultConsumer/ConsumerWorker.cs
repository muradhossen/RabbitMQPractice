using EventBus;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace OrderConsumer.MultConsumer;

public interface IConsumerWorker
{
    Task StartConsuming(string queue, CancellationToken cancellationToken);
}
public class ConsumerWorker : IConsumerWorker
{
    private readonly IRabbitMqConnection _factory;
    //private readonly string _queueName = "order_queue";
    private readonly string _queueName = "order_queue_01";
    
    public string WorkerId { get; } = Guid.NewGuid().ToString("N")[..6];

    public ConsumerWorker(IRabbitMqConnection factory) => _factory = factory;

    public async Task StartConsuming(string queue,CancellationToken cancellationToken)
    {
        try
        {
            // 1. Setup Connection and Channel
            using var connection = await _factory.CreateConnectionAsync(cancellationToken);
            using var channel = await connection.CreateChannelAsync();

            // QoS and other declarations remain the same
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 100, global: false);
            // ... exchange/queue/bind declarations ...

            // 2. Setup Consumer and Subscription
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (sender, ea) =>
            {
                var msg = Encoding.UTF8.GetString(ea.Body.ToArray());
                try
                {
                    Console.WriteLine($"Worker {WorkerId} Received: {msg}");
                    // Simulate work
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Worker {WorkerId} failed: {ex.Message}");
                    // Nack logic
                    //await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
            };

            // Start consuming and keep the task alive until cancellation
            var consumerTag = await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);

            // Keep the task running until cancelled (this is the key)
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (Exception ex)
        {

            throw;
        }
    }
}