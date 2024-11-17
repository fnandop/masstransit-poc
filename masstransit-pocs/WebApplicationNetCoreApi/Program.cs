using MassTransit;
using WebApplicationNetCoreApi.MessageConsumers;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();



builder.Services.AddMassTransit(cfg =>
{
    // Register the consumer and inject dependencies
    cfg.AddConsumer<SomethingApprovedEventConsumer>();

    // Read the transport configuration
    var transportType = builder.Configuration["MassTransit:Transport:Type"];

    if (transportType == "SqlServer")
    {
        builder.Services.AddOptions<SqlTransportOptions>()
            .Configure(options =>
        {
            options.ConnectionString = builder.Configuration["MassTransit:Transport:SqlServer:ConnectionString"];
        });

        var sqlTransportOptions = builder.Configuration.GetSection("MassTransit:Transport:SqlServer")
            .Get<SqlTransportOptions>();

        cfg.UsingSqlServer((context, cfg) =>
        {
            cfg.ReceiveEndpoint("WebApiAppQueue", e =>
            {
                e.Consumer<SomethingApprovedEventConsumer>(context);
            });

            cfg.UseJsonSerializer();
        });

        
    }
    else if (transportType == "RabbitMq")
    {
        // Configure RabbitMq transport
        var rabbitMqHost = builder.Configuration["MassTransit:Transport:RabbitMq:Host"];
        var rabbitMqUsername = builder.Configuration["MassTransit:Transport:RabbitMq:Username"];
        var rabbitMqPassword = builder.Configuration["MassTransit:Transport:RabbitMq:Password"];

        cfg.UsingRabbitMq((context, cfg) =>
        {
            cfg.Host(new Uri(rabbitMqHost!), h =>
            {
                h.Username(rabbitMqUsername!);
                h.Password(rabbitMqPassword!);
            });

            cfg.ReceiveEndpoint("WebApiAppQueue", e =>
            {
                e.Consumer<SomethingApprovedEventConsumer>(context);
            });

            cfg.UseJsonSerializer();
        });
    }
    else
    {
        throw new ArgumentException($"Unsupported transport type: {transportType}");
    }
});



//builder.Services.AddOptions<SqlTransportOptions>()
//    .Configure(options =>
//    {
//        options.ConnectionString = "Data Source=localhost;Initial Catalog=masstransitdb;User ID=sa;Password=bullray1A.;TrustServerCertificate=True";
//    });

//builder.Services.AddMassTransit(cfg =>
//{
//    // Register the consumer and inject dependencies
//    cfg.AddConsumer<SomethingApprovedEventConsumer>();

//    cfg.UsingSqlServer((context, cfg) =>
//    {
//        cfg.ReceiveEndpoint("WebApiAppQueue", e =>
//        {
//            e.Consumer<SomethingApprovedEventConsumer>(context);

//        });
//        cfg.UseJsonSerializer();
//    });
//    cfg.AddSqlServerMigrationHostedService(true, false);





//});


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
