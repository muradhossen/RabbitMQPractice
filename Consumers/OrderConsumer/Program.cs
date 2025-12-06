using EventBus;
using OrderConsumer.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddSingleton(new RabbitMqConnection("localhost", 5672, userName: "admin", password: "admin123"));

builder.Services.AddHostedService<OrderConsumerService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

 
app.MapGet("/weatherforecast", () =>
{
   return Results.Ok("Hello, World!");
})
.WithName("healte-check");

app.Run();

