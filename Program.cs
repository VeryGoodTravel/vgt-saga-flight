﻿using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using NLog;
using NLog.Extensions.Logging;
using RabbitMQ.Client.Exceptions;
using vgt_saga_flight;
using vgt_saga_flight.FlightService;
using vgt_saga_flight.Models;
using ILogger = NLog.ILogger;

var builder = WebApplication.CreateBuilder(args);

AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
try
{
    builder.Configuration.AddJsonFile("appsettings.json", false, true).AddEnvironmentVariables().Build();
}
catch (InvalidDataException e)
{
    Console.WriteLine(e);
    Environment.Exit(0);
}


try
{
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
    var options = new NLogProviderOptions
    {
        AutoShutdown = true
    };
    options.Configure(builder.Configuration.GetSection("NLog"));
    builder.Logging.AddNLog(options);
}
catch (InvalidDataException e)
{
    Console.WriteLine(e);
    Environment.Exit(0);
}

var logger = LogManager.GetCurrentClassLogger();

builder.Services.AddDbContext<FlightDbContext>(options => options.UseNpgsql(SecretUtils.GetConnectionString(builder.Configuration, "DB_NAME_FLIGHT", logger)));

var app = builder.Build();

var lf = app.Services.GetRequiredService<ILoggerFactory>();
logger.Info("Hello word");

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseSwagger();
    app.UseSwaggerUI();
}

await using var scope = app.Services.CreateAsyncScope();
{
    await using var db = scope.ServiceProvider.GetService<FlightDbContext>();
    {
        logger.Info("CAN CONNECT {v}" ,db.Database.CanConnect());
        logger.Info("-------------------------------------------------------------------------------- Before deletion ----------------------------------------------------------");
        //db.Database.EnsureDeleted();
        logger.Info("-------------------------------------------------------------------------------- Before migrations  ----------------------------------------------------------");
        await db.Database.MigrateAsync();
        logger.Info("-------------------------------------------------------------------------------- After migrations before disposes ----------------------------------------------------------");

    }
}

logger.Info("-------------------------------------------------------------------------------- OUTSIDE OF MIGRATIONS SUCCESFULLY DISPOSED ----------------------------------------------------------");

app.UseHttpsRedirection();

FlightService? hotelService = null;

try
{
    logger.Info("-------------------------------------------------------------------------------- Trying setup of the service ----------------------------------------------------------");

    hotelService = new FlightService(app.Configuration, lf);
}
catch (BrokerUnreachableException)
{
    logger.Info("-------------------------------------------------------------------------------- FAILED setup ----------------------------------------------------------");

    GracefulExit(app, logger, [hotelService]);
}
catch (ArgumentException)
{
    logger.Info("-------------------------------------------------------------------------------- Failed setup ----------------------------------------------------------");

    GracefulExit(app, logger, [hotelService]);
}

app.MapPost("/flights", ([FromBody]FlightsRequestHttp request) =>
    {
        using var scope = app.Services.CreateAsyncScope();
        using var db = scope.ServiceProvider.GetService<FlightDbContext>();

        logger.Info("fligths request {v}" ,request);
        
        var dbFlights = from flights in db.Flights
            where request.ArrivalAirportCodes.Equals(flights.ArrivalAirport.AirportCode)
                  && request.DepartureAirportCodes.Equals(flights.DepartureAirport.AirportCode)
                  && flights.FlightTime.Date == request.DepartureDateDt()
                  && (from m in db.Bookings
                      where m.Flight == flights
                      select m.Amount).Sum() + request.NumberOfPassengers < flights.Amount
            select flights;

        logger.Info("fligths results {v}" ,dbFlights);
        var flightsResponse = new List<FlightResponse>();

        foreach (var flight in dbFlights)
        {
            if (flight == null) continue;
            flightsResponse.Add(new FlightResponse
            {
                Available = true,
                FlightId = flight.FlightDbId.ToString(),
                DepartureAirportCode = flight.DepartureAirport.AirportCode,
                DepartureAirportName = flight.DepartureAirport.AirportCity,
                ArrivalAirportCode = flight.ArrivalAirport.AirportCode,
                ArrivalAirportName = flight.ArrivalAirport.AirportCity,
                DepartureDate = flight.FlightTime.ToString(CultureInfo.InvariantCulture),
                Price = 0
            });
        }

        return flightsResponse;
    })
    .WithName("GetFlights")
    .WithOpenApi();

app.MapPost("/flight", ([FromBody]FlightRequestHttp request) =>
    {
        using var scope = app.Services.CreateAsyncScope();
        using var db = scope.ServiceProvider.GetService<FlightDbContext>();

        logger.Info("fligth request {v}" ,request);
        
        var dbFlights = from flights in db.Flights
            where request.FlightId.Equals(flights.FlightDbId.ToString())
                  && request.NumberOfPassengers == flights.Amount
            select flights;

        var flight = dbFlights.FirstOrDefault();
        
        logger.Info("fligth result {v}" ,flight);
        
       return new FlightResponse
            {
                Available = true,
                FlightId = flight.FlightDbId.ToString(),
                DepartureAirportCode = flight.DepartureAirport.AirportCode,
                DepartureAirportName = flight.DepartureAirport.AirportCity,
                ArrivalAirportCode = flight.ArrivalAirport.AirportCode,
                ArrivalAirportName = flight.ArrivalAirport.AirportCity,
                DepartureDate = flight.FlightTime.ToString(),
                Price = 0
            };
    })
    .WithName("GetFlight")
    .WithOpenApi();

app.MapGet("/departure_airports", () =>
    {
        using var scope = app.Services.CreateAsyncScope();
        using var db = scope.ServiceProvider.GetService<FlightDbContext>();

        var dbFlights = from flights in db.Flights
            select flights.DepartureAirport;

        var flightsResponse = new DepartureAirports
        {
            Airports = []
        };
        foreach (var airport in dbFlights.Distinct())
        {
            flightsResponse.Airports.Add(new AirportHttp
            {
                AirportCode = airport.AirportCode,
                AirportName = airport.AirportCity
            });
        }

        flightsResponse.Airports = flightsResponse.Airports.Distinct().ToList();

        return JsonConvert.SerializeObject(flightsResponse);
    })
    .WithName("GetAirports")
    .WithOpenApi();

app.Run();

return;

// dispose objects and close connections
void GracefulExit(WebApplication wA, ILogger log, List<IDisposable?> toDispose)
{
    foreach (var obj in toDispose)
    {
        obj?.Dispose();
    }

    try
    {
        wA.Lifetime.StopApplication();
        AwaitAppStop(wA).Wait();
    }
    catch (ObjectDisposedException)
    {
        log.Info("App already disposed off");
    }

    throw new Exception("Kill the rest of the app");
    
    LogManager.Shutdown();
    Environment.Exit(0);
    throw new Exception("Kill the rest of the app");
}

async Task AwaitAppStop(WebApplication wA)
{
    await wA.StopAsync();
    await wA.DisposeAsync();
}


