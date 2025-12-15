using EventBus;
using OrderService.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSwaggerGen();


//builder.Services.AddSingleton(new RabbitMqConnection("localhost", 5672, userName: "admin", password: "admin123"));
builder.Services.RegisterEventBusServices(builder.Configuration);


builder.Services.AddHostedService<PublisherBackgroundService>();

builder.Services.AddSingleton<SetupQueue>();

builder.Services.AddScoped<PublisherFactory>();

var app = builder.Build(); 

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

 

app.MapGet("/order", async (IRabbitMqConnection connection) =>
{
    SingleQueuePublisher orderPublisher = new SingleQueuePublisher(connection);

    //await orderPublisher.PublishAsync();
})
.WithName("Order");

app.Run();

 