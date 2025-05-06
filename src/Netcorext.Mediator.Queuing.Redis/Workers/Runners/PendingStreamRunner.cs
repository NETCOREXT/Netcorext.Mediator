using FreeRedis;
using Microsoft.Extensions.Logging;
using Netcorext.Mediator.Queuing.Redis.Extensions;
using Netcorext.Mediator.Queuing.Redis.Helpers;
using Netcorext.Mediator.Queuing.Redis.Utilities;
using Netcorext.Serialization;
using Netcorext.Worker;

namespace Netcorext.Mediator.Queuing.Redis.Runners;

internal class PendingStreamRunner : IWorkerRunner<ConsumerWorker>
{
    private readonly IServiceProvider _serviceProvider;
    private readonly MediatorOptions _mediatorOptions;
    private readonly RedisQueuing _queuing;
    private readonly RedisClient _redis;
    private readonly RedisOptions _options;
    private readonly ISerializer _serializer;
    private readonly ILogger<PendingStreamRunner> _logger;
    private readonly SemaphoreSlim _locker = new(1, 1);

    public PendingStreamRunner(IServiceProvider serviceProvider, MediatorOptions mediatorOptions, IQueuing queuing, RedisOptions options, ISerializer serializer, ILogger<PendingStreamRunner> logger)
    {
        _serviceProvider = serviceProvider;
        _mediatorOptions = mediatorOptions;
        _queuing = (RedisQueuing)queuing;
        _redis = _queuing.Redis;
        _options = options;
        _serializer = serializer;
        _logger = logger;
    }

    public async Task InvokeAsync(ConsumerWorker worker, CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_options.PendingCheckInterval ?? RedisOptions.DEFAULT_PENDING_CHECK_INTERVAL, cancellationToken);

            if (!await _locker.WaitAsync(0, cancellationToken))
                continue;

            await _redis.RegisterConsumerAsync(_mediatorOptions.ServiceMaps, _options.Prefix, _options.GroupName, _options.MachineName, _options.GroupNewestId);

            try
            {
                await Task.WhenAll(_mediatorOptions.ServiceMaps
                                                   .Select(service => KeyHelper.Concat(_options.Prefix, service.Interface.GetGenericTypeDefinition() == typeof(IResponseHandler<,>) ? _options.GroupName : string.Empty, service.Service.FullName!))
                                                   .Select(key => ClaimPendingStreamAsync(key, cancellationToken))
                                                   .ToArray());
            }
            finally
            {
                _locker.Release();
            }
        }
    }

    private async Task ClaimPendingStreamAsync(string streamKey, CancellationToken cancellationToken = default)
    {
        var nextId = "-";

        while (!cancellationToken.IsCancellationRequested)
        {
            var pendingResult = (await _redis.XPendingAsync(streamKey, _options.GroupName, nextId, "+", _options.StreamBatchSize ?? RedisOptions.DEFAULT_STREAM_BATCH_SIZE))
                               .Where(t => t.idle > _options.StreamIdleTime)
                               .ToArray();

            if (!pendingResult.Any()) break;

            var pendingIds = pendingResult.Select(t => t.id).ToArray();

            nextId = "(" + pendingResult.Last().id;

            var entries = (await _redis.XClaimAsync(streamKey, _options.GroupName, _options.MachineName, _options.StreamIdleTime ?? RedisOptions.DEFAULT_STREAM_IDLE_TIME, pendingIds))
                         .Select(t => t.ToStreamData())
                         .ToArray();

            if (entries.Length == 0) break;

            foreach (var entry in entries)
            {
                try
                {
                    if (entry.Data == null || !entry.Data.Any()) continue;

                    var rawMessage = await _serializer.DeserializeAsync<Message>(entry.Data, cancellationToken);

                    if (rawMessage == null) continue;

                    var message = rawMessage.Referer == null
                                      ? new Message
                                        {
                                            ServiceType = rawMessage.ServiceType,
                                            RefererType = rawMessage.PayloadType,
                                            Referer = rawMessage.Payload,
                                            GroupName = _options.GroupName,
                                            MachineName = _options.MachineName,
                                            Authorization = rawMessage.Authorization,
                                            RequestId = rawMessage.RequestId
                                        }
                                      : new Message
                                        {
                                            ServiceType = rawMessage.ServiceType,
                                            RefererType = rawMessage.RefererType,
                                            Referer = rawMessage.Referer,
                                            GroupName = _options.GroupName,
                                            MachineName = _options.MachineName,
                                            Authorization = rawMessage.Authorization,
                                            RequestId = rawMessage.RequestId
                                        };

                    try
                    {
                        var result = await StreamDataSender.SendAsync(_serviceProvider, _queuing, entry, cancellationToken);

                        if (!rawMessage.Respond.HasValue || !rawMessage.Respond.Value || rawMessage.Referer != null) continue;

                        message.PayloadType = result?.GetType().AssemblyQualifiedName;
                        message.Payload = await _serializer.SerializeToUtf8BytesAsync(result, cancellationToken);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "{Message}", e.Message);

                        message.Error = e.ToString();
                    }

                    message.CreationDate = DateTimeOffset.UtcNow;

                    var publishKey = KeyHelper.GetStreamKey(KeyHelper.Concat(_options.Prefix, rawMessage.GroupName), rawMessage.ServiceType!);

                    await _queuing.PublishAsync(publishKey, message, cancellationToken);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "{Message}", e.Message);
                }
            }
        }

        var consumers = await _redis.XInfoConsumersAsync(streamKey, _options.GroupName);

        var pendingConsumers = consumers.Where(t => t.name != _options.MachineName && t.idle > _options.StreamIdleTime)
                                        .ToArray();

        foreach (var consumer in pendingConsumers)
        {
            await _redis.XGroupDelConsumerAsync(streamKey, _options.GroupName, consumer.name);
        }
    }

    public void Dispose()
    {
        _locker.Dispose();
    }
}
