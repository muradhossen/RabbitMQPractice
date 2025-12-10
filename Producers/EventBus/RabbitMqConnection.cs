using RabbitMQ.Client;

namespace EventBus;

public interface IRabbitMqConnection
{
    Task<IConnection> CreateConnectionAsync(CancellationToken token);
}


public class RabbitMqConnection : IRabbitMqConnection
{
    private readonly ConnectionFactory _factory;

    public RabbitMqConnection(string hostName,int port, string userName, string password)
    {
        _factory = new ConnectionFactory
        {
            HostName = hostName,
            Port = port,
            UserName = userName,
            Password = password,
        };
    }

    public async Task<IConnection> CreateConnectionAsync(CancellationToken token)
    {
        return await _factory.CreateConnectionAsync(token);
    }
}
