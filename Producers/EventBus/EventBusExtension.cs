using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EventBus;

public static class EventBusExtension
{
    public static IServiceCollection RegisterEventBusServices(this IServiceCollection services, IConfiguration configuration)
    { 
        var connection = new RabbitMqConnection("localhost", 5672, userName: "admin", password: "admin123");

        services.AddSingleton<IRabbitMqConnection>(sp => connection);

        return services;
    }
}
