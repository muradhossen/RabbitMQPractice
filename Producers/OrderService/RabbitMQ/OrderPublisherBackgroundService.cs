
using EventBus;

namespace OrderService.RabbitMQ;

public class OrderPublisherBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;

    public OrderPublisherBackgroundService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var connection = scope.ServiceProvider.GetRequiredService<RabbitMqConnection>();

        OrderPublisher orderPublisher = new OrderPublisher(connection);
        int iterration = 0; 

        while (!stoppingToken.IsCancellationRequested)
        {
            await orderPublisher.PublishAsync($"Order created at {DateTime.UtcNow} -- NEW {iterration}", stoppingToken);
            
            iterration++;
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }

    }
}
