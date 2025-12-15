
using EventBus;
using OrderService.Models;

namespace OrderService.RabbitMQ;

public class PublisherBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;

    public PublisherBackgroundService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope(); 

        var publisherFactory = scope.ServiceProvider.GetRequiredService<PublisherFactory>();
        var publisher = publisherFactory.CreatePublisherService(PublisherType.OrderSingleQueue);
        await publisher.StartTask(stoppingToken);

    } 

}

public interface IPublisherService
{
    Task StartTask(CancellationToken stoppingToken = default);
}

public class OrderSinglePublisherService : IPublisherService
{
    private readonly IServiceProvider _serviceProvider;
    public OrderSinglePublisherService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }
    public async Task StartTask(CancellationToken stoppingToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var queue = scope.ServiceProvider.GetRequiredService<SetupQueue>();

        var config = new QueueConfig
        {
            ExchangeName = "order_exchange",
            QueueName = "order_queue",          
            RoutingKey = "order_queue"
        };

        await queue.SetupSingleQueue(config, stoppingToken);

        await ProduceAsync(config,stoppingToken);
    }

    private async Task ProduceAsync(QueueConfig config , CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var connection = scope.ServiceProvider.GetRequiredService<IRabbitMqConnection>();

        SingleQueuePublisher orderPublisher = new SingleQueuePublisher(connection);
        var order = FakeOrderGenerator.Generate(1, Enumerable.Range(1, 1000).ToList()).First(); 

        while (!stoppingToken.IsCancellationRequested)
        {
            var message = new Message<Order>(config.ExchangeName, config.QueueName, config.RoutingKey, order);

            await orderPublisher.PublishAsync(message, stoppingToken);

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}

public class OrderMultiPublisherService : IPublisherService
{
    private readonly IServiceProvider _serviceProvider;
    public OrderMultiPublisherService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }
    public async Task StartTask(CancellationToken stoppingToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var queue = scope.ServiceProvider.GetRequiredService<SetupQueue>();

        var config = new QueueConfig { ExchangeName = "sharded_order_exchange", QueueName = "order_queue", NumberOfShards = 5 };
        await queue.SetupMultiQueue(config, stoppingToken);

        await ProduceAsync(config.ExchangeName, stoppingToken);
    }
    private async Task ProduceAsync(string exchange, CancellationToken stoppingToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var connection = scope.ServiceProvider.GetRequiredService<IRabbitMqConnection>();

        MultiQueuePublisher orderPublisher = new MultiQueuePublisher(connection);
        var orders = FakeOrderGenerator.Generate(10000, Enumerable.Range(1, 1000).ToList());


        while (!stoppingToken.IsCancellationRequested)
        {

            foreach (var order in orders)
            {
                var message = new Message<Order>(exchange, queue: "", order.CustomerId.ToString(), order);

                await orderPublisher.PublishAsync(message, stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }

    }
}

public class PublisherFactory
{
    private readonly IServiceProvider _serviceProvider;
    public PublisherFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }
    public IPublisherService CreatePublisherService(PublisherType type)
    {
        if (PublisherType.OrderMultiQueue == type)
        {
            return new OrderMultiPublisherService(_serviceProvider);
        }
        else if (PublisherType.OrderSingleQueue == type)
        {
            return new OrderSinglePublisherService(_serviceProvider);
        }
        throw new NotImplementedException("Type not implemented!");
    }
}

public enum PublisherType
{
    OrderMultiQueue = 1,
    OrderSingleQueue = 2
}