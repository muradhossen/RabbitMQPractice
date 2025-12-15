namespace OrderService.Models;

public class Message <T>(string exchange, string queue, string routingKey, T payload) where T : class
{ 
    public string Exchange { get; init; } = exchange;
    public  string RoutingKey { get; init; } = routingKey;
    public  string Queue { get; init; } = queue;
    public  T Payload { get; init; } = payload;
}
