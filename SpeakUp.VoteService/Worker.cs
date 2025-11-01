using SpeakUp.Common;
using SpeakUp.Common.Events.Entry;
using SpeakUp.Common.Events.EntryComment;
using SpeakUp.Common.Infratructure;
using System.Text;
using System.Text.Json;

namespace SpeakUp.VoteService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> logger;
    private readonly IConfiguration configuration;

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        this.logger = logger;
        this.configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connStr = configuration.GetConnectionString("SqlServer");
        var voteService = new Services.VoteService(connStr);

        var createEntryConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.VoteExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.CreateEntryVoteQueueName, SpeakUpConstants.VoteExchangeName);

        createEntryConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var vote = JsonSerializer.Deserialize<CreateEntryVoteEvent>(message);

                await voteService.CreateEntryVote(vote);
                logger.LogInformation("Create Entry Received EntryId: {0}, VoteType: {1}",
                    vote.EntryId, vote.VoteType);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing CreateEntryVoteEvent");
            }
        };

        await createEntryConsumer.StartConsumingAsync(SpeakUpConstants.CreateEntryVoteQueueName);

        var deleteEntryConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.VoteExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.DeleteEntryVoteQueueName, SpeakUpConstants.VoteExchangeName);

        deleteEntryConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var vote = JsonSerializer.Deserialize<DeleteEntryVoteEvent>(message);

                await voteService.DeleteEntryVote(vote.EntryId, vote.CreatedBy);
                logger.LogInformation("Delete Entry Received EntryId: {0}", vote.EntryId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing DeleteEntryVoteEvent");
            }
        };

        await deleteEntryConsumer.StartConsumingAsync(SpeakUpConstants.DeleteEntryVoteQueueName);

        var createCommentConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.VoteExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.CreateEntryCommentVoteQueueName, SpeakUpConstants.VoteExchangeName);

        createCommentConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var vote = JsonSerializer.Deserialize<CreateEntryCommentVoteEvent>(message);

                await voteService.CreateEntryCommentVote(vote);
                logger.LogInformation("Create Entry Comment Received EntryCommentId: {0}, VoteType: {1}",
                    vote.EntryCommentId, vote.VoteType);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing CreateEntryCommentVoteEvent");
            }
        };

        await createCommentConsumer.StartConsumingAsync(SpeakUpConstants.CreateEntryCommentVoteQueueName);

        var deleteCommentConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.VoteExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.DeleteEntryCommentVoteQueueName, SpeakUpConstants.VoteExchangeName);

        deleteCommentConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var vote = JsonSerializer.Deserialize<DeleteEntryCommentVoteEvent>(message);

                await voteService.DeleteEntryCommentVote(vote.EntryCommentId, vote.CreatedBy);
                logger.LogInformation("Delete Entry Comment Received EntryCommentId: {0}", vote.EntryCommentId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing DeleteEntryCommentVoteEvent");
            }
        };

        await deleteCommentConsumer.StartConsumingAsync(SpeakUpConstants.DeleteEntryCommentVoteQueueName);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}