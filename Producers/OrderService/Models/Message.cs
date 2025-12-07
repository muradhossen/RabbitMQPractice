namespace OrderService.Models;

public class Message <T> where T : class
{
    public int RoutingKey { get; set; }
    public T Payload { get; set; }
}
