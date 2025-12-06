using RabbitMQ.Client;

namespace EventBus;

public class RabbitMqConnection
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
