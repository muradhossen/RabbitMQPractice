using EventBus;
using OrderConsumer.MultConsumer;
using OrderConsumer.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddSingleton(new RabbitMqConnection("localhost", 5672, userName: "admin", password: "admin123"));

//builder.Services.AddHostedService<OrderConsumerService>();

builder.Services.AddSingleton<IConsumerWorker, ConsumerWorker>();
builder.Services.AddSingleton<ConsumerManagerService>();


var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.MapGet("/scale-up", (ConsumerManagerService manager) =>
{
    int newCount = manager.StartNewConsumer();
    return Results.Ok(new { message = "New consumer thread started successfully.", totalConsumers = newCount });
})
.WithName("ScaleUpConsumer");

app.Run();

