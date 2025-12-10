using EventBus;
using Microsoft.AspNetCore.Mvc;
using OrderConsumer.MultConsumer;
using OrderConsumer.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

//builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();

//builder.Services.AddSingleton(new RabbitMqConnection("localhost", 5672, userName: "admin", password: "admin123"));

builder.Services.RegisterEventBusServices(builder.Configuration);


//builder.Services.AddHostedService<OrderConsumerService>();

builder.Services.AddSingleton<IConsumerWorker, ConsumerWorker>();
builder.Services.AddSingleton<ConsumerManagerService>();


var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();


app.MapGet("/scale-up/{queue}", ([FromRoute] string queue,ConsumerManagerService manager) =>
{
    int newCount = manager.StartNewConsumer(queue);
    return Results.Ok(new { message = "New consumer thread started successfully.", totalConsumers = newCount });
})
.WithName("ScaleUpConsumer");

app.MapGet("/stop-all", async (ConsumerManagerService manager) =>
{
    await manager.StopAllConsumers();
    return Results.Ok(new { message = "All consumers are stopped.", totalConsumers = manager.ActiveConsumersCount });
})
.WithName("StopAllConsumer");

app.MapGet("/consumers", (ConsumerManagerService manager) =>
{
    var consumers = manager.ActiveConsumers.Select(c => new
    {
        c.Id,
        c.QueueName,
        IsRunning = !c.Task.IsCompleted
    }).OrderBy(c => c.QueueName);
    return Results.Ok(consumers);
});

app.MapDelete("/consumers/{id}", async (string id,ConsumerManagerService manager) =>
{
    await manager.StopConsumer(id);
    return Results.Ok($"{manager.ActiveConsumersCount}");
});

app.Run();

