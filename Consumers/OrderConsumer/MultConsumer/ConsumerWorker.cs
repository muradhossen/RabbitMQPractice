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
    
    public string WorkerId { get; } = Guid.NewGuid().ToString("N")[..6];

    public ConsumerWorker(IRabbitMqConnection factory) => _factory = factory;

    public async Task StartConsuming(string queue,CancellationToken cancellationToken)
    {
        try
        {
            using var connection = await _factory.CreateConnectionAsync(cancellationToken);
            using var channel = await connection.CreateChannelAsync();

           
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 100, global: false);
          
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (sender, ea) =>
            {
                var msg = Encoding.UTF8.GetString(ea.Body.ToArray());
                try
                {
                    Console.WriteLine($"Worker {WorkerId} Received: {msg}");
                   
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Worker {WorkerId} failed: {ex.Message}");
                    // Nack logic
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
            };

            var consumerTag = await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);

            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (Exception ex)
        {

            throw;
        }
    }
}