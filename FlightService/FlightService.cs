using System.Threading.Channels;
using Microsoft.EntityFrameworkCore;
using NEventStore;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using vgt_saga_flight.Models;
using vgt_saga_serialization;

namespace vgt_saga_flight.FlightService;

/// <summary>
/// Saga Payment service;
/// handles all payments in the transaction.
/// </summary>
public class FlightService : IDisposable
{
    private readonly FlightQueueHandler _queues;
    private readonly Logger _logger;
    private readonly IConfiguration _config;
    private readonly Utils _jsonUtils;
    private readonly IStoreEvents _eventStore;
    
    private readonly Channel<Message> _payments;
    private readonly Channel<Message> _publish;
    private readonly FlightHandler _flightHandler;

    private readonly FlightDbContext _writeDb;
    private readonly FlightDbContext _readDb;
    
    private Task Publisher { get; set; }
    
    /// <summary>
    /// Allows tasks cancellation from the outside of the class
    /// </summary>
    public CancellationToken Token { get; } = new();

    /// <summary>
    /// Constructor of the FlightService class.
    /// Initializes FlightService object.
    /// Creates, initializes and opens connections to the database and rabbitmq
    /// based on configuration data present and handled by specified handling objects.
    /// Throws propagated exceptions if the configuration data is nowhere to be found.
    /// </summary>
    /// <param name="config"> Configuration with the connection params </param>
    /// <param name="lf"> Logger factory to use by the event store </param>
    /// <exception cref="ArgumentException"> Which variable is missing in the configuration </exception>
    /// <exception cref="BrokerUnreachableException"> Couldn't establish connection with RabbitMQ </exception>
    public FlightService(IConfiguration config, ILoggerFactory lf)
    {
        _logger = LogManager.GetCurrentClassLogger();
        _config = config;

        _jsonUtils = new Utils(_logger);
        _payments = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions()
            { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true });
        _logger.Info("-------------------------------------------------------------------------------- Creating service connection ----------------------------------------------------------");

        var connStr = SecretUtils.GetConnectionString(_config, "DB_NAME_FLIGHT", _logger);
        var op = new DbContextOptions<FlightDbContext>();
        //op.UseLoggerFactory(lf);
        _logger.Info("-------------------------------------------------------------------------------- creting write db context ----------------------------------------------------------");

        _writeDb = new FlightDbContext(op, connStr);
        
        _logger.Info("-------------------------------------------------------------------------------- creating read db context ----------------------------------------------------------");
        _readDb = new FlightDbContext(op, connStr);
        
        _logger.Info("-------------------------------------------------------------------------------- Created db connections ----------------------------------------------------------");

        
        if (!_readDb.Flights.Any())
        {
            CreateData().Wait(); 
        }
        
        _logger.Info("-------------------------------------------------------------------------------- Created db data ----------------------------------------------------------");
        Publisher = Task.Run(() => RabbitPublisher());
        
        _publish = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions()
            { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true });
        
        _flightHandler = new FlightHandler(_payments, _publish, _writeDb, _readDb, _logger);

        _queues = new FlightQueueHandler(_config, _logger);
        
        _queues.AddRepliesConsumer(SagaOrdersEventHandler);
    }
    
    private async Task CreateData()
    {
        using StreamReader departure = new("./departure_airports.json");
        var json = await departure.ReadToEndAsync(Token);
        Dictionary<string,string> departureAirports = JsonConvert.DeserializeObject<Dictionary<string,string>>(json) ?? [];
        
        using StreamReader arrival = new("./arrival_airports.json");
        var json2 = await arrival.ReadToEndAsync(Token);
        Dictionary<string,string> arrivalAirports = JsonConvert.DeserializeObject<Dictionary<string,string>>(json2) ?? [];
        var rnd = new Random();
        
        
        var departureDbAirports = departureAirports.Select(airport => new AirportDb { AirportCode = airport.Key, AirportCity = airport.Value, IsDeparture = true }).ToList();
        _writeDb.AddRange(departureDbAirports);
        var arrivalDbAirports = arrivalAirports.Select(airport => new AirportDb { AirportCode = airport.Key, AirportCity = airport.Value, IsDeparture = false }).ToList();
        _writeDb.AddRange(arrivalDbAirports);

        List<FlightDb> flights = [];
        foreach (var airport in departureDbAirports)
        {
            for (var i = 0; i < rnd.Next(1000, 1500); i++)
            {
                _writeDb.Add(new FlightDb
                {
                    Amount = rnd.Next(5, 25),
                    FlightTime = DateTime.Now + TimeSpan.FromMinutes(rnd.Next(0, 200000)),
                    ArrivalAirport = arrivalDbAirports[rnd.Next(0, arrivalDbAirports.Count - 1)],
                    DepartureAirport = airport,
                    Price = rnd.Next(80, 250)
                });
            }
            // presentation flights
            _writeDb.Add(new FlightDb
            {
                Amount = rnd.Next(5, 25),
                FlightTime = DateTime.Now,
                ArrivalAirport = arrivalDbAirports[rnd.Next(0, arrivalDbAirports.Count - 1)],
                DepartureAirport = airport,
                Price = rnd.Next(80, 250)
            });
            _writeDb.Add(new FlightDb
            {
                Amount = rnd.Next(5, 25),
                FlightTime = DateTime.Now + TimeSpan.FromDays(3),
                ArrivalAirport = arrivalDbAirports[rnd.Next(0, arrivalDbAirports.Count - 1)],
                DepartureAirport = airport,
                Price = rnd.Next(80, 250)
            });
        }
        foreach (var airport in arrivalDbAirports)
        {
            for (var i = 0; i < rnd.Next(200, 1000); i++)
            {
                _writeDb.Add(new FlightDb
                {
                    Amount = rnd.Next(5, 25),
                    FlightTime = DateTime.Now + TimeSpan.FromMinutes(rnd.Next(0, 200000)),
                    ArrivalAirport = departureDbAirports[rnd.Next(0, departureDbAirports.Count - 1)],
                    DepartureAirport = airport,
                    Price = rnd.Next(80, 250)
                });
            }
            // presentation flights
            _writeDb.Add(new FlightDb
            {
                Amount = rnd.Next(5, 25),
                FlightTime = DateTime.Now,
                ArrivalAirport = departureDbAirports[rnd.Next(0, departureDbAirports.Count - 1)],
                DepartureAirport = airport,
                Price = rnd.Next(80, 250)
            });
            _writeDb.Add(new FlightDb
            {
                Amount = rnd.Next(5, 25),
                FlightTime = DateTime.Now + TimeSpan.FromDays(3),
                ArrivalAirport = departureDbAirports[rnd.Next(0, departureDbAirports.Count - 1)],
                DepartureAirport = airport,
                Price = rnd.Next(80, 250)
            });
        }
        await _writeDb.SaveChangesAsync(Token);
    }

    /// <summary>
    /// Publishes made messages to the right queues
    /// </summary>
    private async Task RabbitPublisher()
    {
        _logger.Debug("-----------------Rabbit publisher starting");
        while (await _publish.Reader.WaitToReadAsync(Token))
        {
            _logger.Debug("-----------------Rabbit publisher message");
            var message = await _publish.Reader.ReadAsync(Token);
            _logger.Debug("Recieved message {msg} {id}", message.MessageType.ToString(), message.TransactionId);
            
            _queues.PublishToOrchestrator( _jsonUtils.Serialize(message));
        }
    }

    /// <summary>
    /// Event Handler that hooks to the event of the queue consumer.
    /// Handles incoming replies from the RabbitMQ and routes them to the appropriate tasks.
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="ea"></param>
    private void SagaOrdersEventHandler(object? sender, BasicDeliverEventArgs ea)
    {
        _logger.Debug("Received response | Tag: {tag}", ea.DeliveryTag);
        var body = ea.Body.ToArray();

        var reply = _jsonUtils.Deserialize(body);

        if (reply == null) return;

        var message = reply.Value;

        // send message reply to the appropriate task
        var result = _payments.Writer.TryWrite(message);
        
        if (result) _logger.Debug("Replied routed successfuly to {type} handler", message.MessageType.ToString());
        else _logger.Warn("Something went wrong in routing to {type} handler", message.MessageType.ToString());

        _queues.PublishTagResponse(ea, result);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _logger.Debug("Disposing");
        _queues.Dispose();
    }
}