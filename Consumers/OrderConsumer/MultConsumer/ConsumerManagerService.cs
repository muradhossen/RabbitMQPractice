using EventBus;

namespace OrderConsumer.MultConsumer;

public class ConsumerManagerService
{
    private readonly RabbitMqConnection _factory;
    // Holds all running consumer tasks for management
    private readonly List<Task> _consumerTasks = new();
    private readonly CancellationTokenSource _cts = new();

    public ConsumerManagerService(RabbitMqConnection factory) => _factory = factory;

    // The method called by the API endpoint to scale up
    public int StartNewConsumer()
    {
        var worker = new ConsumerWorker(_factory);
        // Start the consuming logic as a long-running Task
        var task = worker.StartConsuming(_cts.Token);
        _consumerTasks.Add(task);

        // Clean up completed tasks (optional, but good practice)
        _consumerTasks.RemoveAll(t => t.IsCompleted);

        Console.WriteLine($"New consumer started. Total active consumers: {_consumerTasks.Count}");
        return _consumerTasks.Count;
    }

    // Optional: Method to stop all workers gracefully when the host shuts down
    public async Task StopAllConsumers()
    {
        _cts.Cancel();
        try
        {
            await Task.WhenAll(_consumerTasks.ToArray());
        }
        catch (OperationCanceledException)
        {
            // Expected when the tasks are cancelled
        }
    }
}