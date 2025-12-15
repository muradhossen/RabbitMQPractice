using EventBus;

namespace OrderConsumer.MultConsumer;

public class ConsumerManagerService
{
    private readonly IRabbitMqConnection _factory;
    private readonly List<Task> _consumerTasks = new();
  

    private List<ConsumerInfo> Consumers = new List<ConsumerInfo>();
    public int ActiveConsumersCount => Consumers.Count;
    public IEnumerable<ConsumerInfo> ActiveConsumers => Consumers;

    public ConsumerManagerService(IRabbitMqConnection factory) => _factory = factory;

    public int StartNewConsumer(string queue)
    {

         CancellationTokenSource _cts = new();
        var worker = new ConsumerWorker(_factory);

        var task = worker.StartConsuming(queue, _cts.Token);
        _consumerTasks.Add(task);

        _consumerTasks.RemoveAll(t => t.IsCompleted);

        Consumers.Add(new ConsumerInfo(worker.WorkerId, queue, task, _cts));

        Console.WriteLine($"New consumer started. Total active consumers: {_consumerTasks.Count}");
        return _consumerTasks.Count;
    } 
 
    public async Task StopAllConsumers()
    {

        foreach (var consumer in Consumers)
        {
            consumer.Source.Cancel();
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

    public async Task StopConsumer(string consumerId)
    {
        var consumer = Consumers.FirstOrDefault(c => c.Id == consumerId);
        if (consumer != null)
        {
            consumer.Source.Cancel();
            try
            {
                await consumer.Task;
            }
            catch (OperationCanceledException)
            {
                // Expected when the task is cancelled
            }
            Consumers.Remove(consumer); 
            _consumerTasks.RemoveAll(t => t.IsCompleted);
        }
    }

}


public record ConsumerInfo(string Id, string QueueName, Task Task, CancellationTokenSource Source);