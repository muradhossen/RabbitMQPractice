using EventBus;
using OrderService.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSwaggerGen();


//builder.Services.AddSingleton(new RabbitMqConnection("localhost", 5672, userName: "admin", password: "admin123"));
builder.Services.RegisterEventBusServices(builder.Configuration);


builder.Services.AddHostedService<OrderPublisherBackgroundService>();

builder.Services.AddSingleton<SetupQueue>();

var app = builder.Build(); 

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

 

app.MapGet("/order", async (IRabbitMqConnection connection) =>
{
    OrderPublisher orderPublisher = new OrderPublisher(connection);

    await orderPublisher.PublishAsync($"Order created at {DateTime.UtcNow}");
})
.WithName("Order");

app.Run();

 