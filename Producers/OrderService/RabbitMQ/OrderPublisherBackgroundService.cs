
using EventBus;
using OrderService.Models;

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
        var queue = scope.ServiceProvider.GetRequiredService<SetupQueue>();
        await queue.SetupRabbitMqAsync(stoppingToken);

        //await SingleQueue(stoppingToken);

        await MultiQueue(stoppingToken);

    }

    private async Task SingleQueue(CancellationToken stoppingToken)
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

    private async Task MultiQueue(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var connection = scope.ServiceProvider.GetRequiredService<IRabbitMqConnection>();

        OrderPublisherMultiQueue orderPublisher = new OrderPublisherMultiQueue(connection);


        var orders = FakeOrderGenerator.Generate(10000, Enumerable.Range(1, 1000).ToList());


        while (!stoppingToken.IsCancellationRequested)
        {

            foreach (var order in orders)
            {
                var message = new Message<Order>
                {
                    RoutingKey = order.CustomerId,
                    Payload = order
                };

                await orderPublisher.PublishAsync(message, stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }

    }
}
