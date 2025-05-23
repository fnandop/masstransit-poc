
USE [masstransitdb]
GO
/****** Object:  DatabaseRole [transport]    Script Date: 19/11/2024 23:30:28 ******/
CREATE ROLE [transport]
GO
/****** Object:  Schema [transport]    Script Date: 19/11/2024 23:30:28 ******/
CREATE SCHEMA [transport]
GO
USE [masstransitdb]
GO
/****** Object:  Sequence [transport].[DeliverySequence]    Script Date: 19/11/2024 23:30:28 ******/
CREATE SEQUENCE [transport].[DeliverySequence] 
 AS [bigint]
 START WITH 1
 INCREMENT BY 1
 MINVALUE -9223372036854775808
 MAXVALUE 9223372036854775807
 CACHE 
GO
USE [masstransitdb]
GO
/****** Object:  Sequence [transport].[TopologySequence]    Script Date: 19/11/2024 23:30:28 ******/
CREATE SEQUENCE [transport].[TopologySequence] 
 AS [bigint]
 START WITH 1
 INCREMENT BY 1
 MINVALUE -9223372036854775808
 MAXVALUE 9223372036854775807
 CACHE 
GO
/****** Object:  Table [transport].[Queue]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[Queue](
	[Id] [bigint] NOT NULL,
	[Updated] [datetime2](7) NOT NULL,
	[Name] [nvarchar](256) NOT NULL,
	[Type] [tinyint] NOT NULL,
	[AutoDelete] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [transport].[MessageDelivery]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[MessageDelivery](
	[MessageDeliveryId] [bigint] NOT NULL,
	[TransportMessageId] [uniqueidentifier] NOT NULL,
	[QueueId] [bigint] NOT NULL,
	[Priority] [smallint] NOT NULL,
	[EnqueueTime] [datetime2](7) NOT NULL,
	[ExpirationTime] [datetime2](7) NULL,
	[PartitionKey] [nvarchar](128) NULL,
	[RoutingKey] [nvarchar](256) NULL,
	[ConsumerId] [uniqueidentifier] NULL,
	[LockId] [uniqueidentifier] NULL,
	[DeliveryCount] [int] NOT NULL,
	[MaxDeliveryCount] [int] NOT NULL,
	[LastDelivered] [datetime2](7) NULL,
	[TransportHeaders] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[MessageDeliveryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [transport].[QueueMetric]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[QueueMetric](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[StartTime] [datetime2](7) NOT NULL,
	[Duration] [int] NOT NULL,
	[QueueId] [bigint] NOT NULL,
	[ConsumeCount] [bigint] NOT NULL,
	[ErrorCount] [bigint] NOT NULL,
	[DeadLetterCount] [bigint] NOT NULL,
 CONSTRAINT [PK_QueueMetric] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [transport].[Queues]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [transport].[Queues]
AS
SELECT x.QueueName,
       MAX(x.QueueAutoDelete)                AS QueueAutoDelete,
       SUM(x.MessageReady)                   AS Ready,
       SUM(x.MessageScheduled)               AS Scheduled,
       SUM(x.MessageError)                   AS Errored,
       SUM(x.MessageDeadLetter)              AS DeadLettered,
       SUM(x.MessageLocked)                  AS Locked,
       ISNULL(MAX(x.ConsumeCount), 0)        AS ConsumeCount,
       ISNULL(MAX(x.ErrorCount), 0)          AS ErrorCount,
       ISNULL(MAX(x.DeadLetterCount), 0)     AS DeadLetterCount,
       MAX(x.StartTime)                      AS CountStartTime,
       ISNULL(MAX(x.Duration), 0)            AS CountDuration
FROM (SELECT q.Name                                              AS QueueName,
             q.AutoDelete                                        AS QueueAutoDelete,
             qm.ConsumeCount,
             qm.ErrorCount,
             qm.DeadLetterCount,
             qm.StartTime,
             qm.Duration,

             IIF(q.Type = 1
                     AND md.MessageDeliveryId IS NOT NULL
                     AND md.EnqueueTime <= GETUTCDATE(), 1, 0)   AS MessageReady,
             IIF(q.Type = 1
                     AND md.MessageDeliveryId IS NOT NULL
                     AND md.LockId IS NULL
                     AND md.EnqueueTime > GETUTCDATE(), 1, 0)    AS MessageScheduled,
             IIF(q.Type = 1
                     AND md.MessageDeliveryId IS NOT NULL
                     AND md.LockId IS NOT NULL
                     AND md.DeliveryCount >= 1
                     AND md.EnqueueTime > GETUTCDATE(), 1, 0)    AS MessageLocked,
             IIF(q.Type = 2
                     AND md.MessageDeliveryId IS NOT NULL, 1, 0) AS MessageError,
             IIF(q.Type = 3
                     AND md.MessageDeliveryId IS NOT NULL, 1, 0) AS MessageDeadLetter
      FROM transport.Queue q
               LEFT JOIN transport.MessageDelivery md ON q.Id = md.QueueId
               LEFT JOIN (SELECT qm.QueueId,
                                 qm.QueueName,
                                 qm.ConsumeCount    AS ConsumeCount,
                                 qm.ErrorCount      AS ErrorCount,
                                 qm.DeadLetterCount AS DeadLetterCount,
                                 qm.StartTime,
                                 qm.Duration
                          FROM (SELECT qm.QueueId,
                                       q2.Name                                                                as QueueName,
                                       ROW_NUMBER() OVER (PARTITION BY qm.QueueId ORDER BY qm.StartTime DESC) AS RowNum,
                                       qm.ConsumeCount,
                                       qm.ErrorCount,
                                       qm.DeadLetterCount,
                                       qm.StartTime,
                                       qm.Duration
                                FROM transport.QueueMetric qm
                                         INNER JOIN transport.Queue q2 ON qm.QueueId = q2.Id
                                WHERE q2.Type = 1
                                  AND qm.StartTime >= DATEADD(MINUTE, -5, GETUTCDATE())) qm
                          WHERE qm.RowNum = 1) qm ON qm.QueueId = q.Id) x
GROUP BY x.QueueName;
GO
/****** Object:  Table [transport].[Topic]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[Topic](
	[Id] [bigint] NOT NULL,
	[Updated] [datetime2](7) NOT NULL,
	[Name] [nvarchar](256) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [transport].[TopicSubscription]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[TopicSubscription](
	[Id] [bigint] NOT NULL,
	[Updated] [datetime2](7) NOT NULL,
	[SourceId] [bigint] NOT NULL,
	[DestinationId] [bigint] NOT NULL,
	[SubType] [tinyint] NOT NULL,
	[RoutingKey] [nvarchar](256) NOT NULL,
	[Filter] [nvarchar](1024) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [transport].[QueueSubscription]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[QueueSubscription](
	[Id] [bigint] NOT NULL,
	[Updated] [datetime2](7) NOT NULL,
	[SourceId] [bigint] NOT NULL,
	[DestinationId] [bigint] NOT NULL,
	[SubType] [tinyint] NOT NULL,
	[RoutingKey] [nvarchar](256) NOT NULL,
	[Filter] [nvarchar](1024) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [transport].[Subscriptions]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [transport].[Subscriptions]
AS
    SELECT t.name as TopicName, 'topic' as DestinationType,  t2.name as DestinationName, ts.SubType as SubscriptionType, ts.RoutingKey
    FROM transport.topic t
             JOIN transport.TopicSubscription ts ON t.id = ts.sourceid
             JOIN transport.topic t2 on t2.id = ts.destinationid
    UNION
    SELECT t.name as TopicName, 'queue' as DestinationType, q.name as DestinationName, qs.SubType as SubscriptionType, qs.RoutingKey
    FROM transport.queuesubscription qs
             LEFT JOIN transport.queue q on qs.destinationid = q.id
             LEFT JOIN transport.topic t on qs.sourceid = t.id;
GO
/****** Object:  Table [transport].[Message]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[Message](
	[TransportMessageId] [uniqueidentifier] NOT NULL,
	[ContentType] [nvarchar](max) NULL,
	[MessageType] [nvarchar](max) NULL,
	[Body] [nvarchar](max) NULL,
	[BinaryBody] [varbinary](max) NULL,
	[MessageId] [uniqueidentifier] NULL,
	[CorrelationId] [uniqueidentifier] NULL,
	[ConversationId] [uniqueidentifier] NULL,
	[RequestId] [uniqueidentifier] NULL,
	[InitiatorId] [uniqueidentifier] NULL,
	[SourceAddress] [nvarchar](max) NULL,
	[DestinationAddress] [nvarchar](max) NULL,
	[ResponseAddress] [nvarchar](max) NULL,
	[FaultAddress] [nvarchar](max) NULL,
	[SentTime] [datetime2](7) NOT NULL,
	[Headers] [nvarchar](max) NULL,
	[Host] [nvarchar](max) NULL,
	[SchedulingTokenId] [uniqueidentifier] NULL,
PRIMARY KEY CLUSTERED 
(
	[TransportMessageId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [transport].[QueueMetricCapture]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [transport].[QueueMetricCapture](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Captured] [datetime2](7) NOT NULL,
	[QueueId] [bigint] NOT NULL,
	[ConsumeCount] [bigint] NOT NULL,
	[ErrorCount] [bigint] NOT NULL,
	[DeadLetterCount] [bigint] NOT NULL,
 CONSTRAINT [PK_QueueMetricCapture] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Index [IX_Message_SchedulingTokenId]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_Message_SchedulingTokenId] ON [transport].[Message]
(
	[SchedulingTokenId] ASC
)
WHERE ([Message].[SchedulingTokenId] IS NOT NULL)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_MessageDelivery_Fetch]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_MessageDelivery_Fetch] ON [transport].[MessageDelivery]
(
	[QueueId] ASC,
	[Priority] ASC,
	[EnqueueTime] ASC,
	[MessageDeliveryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_MessageDelivery_FetchPart]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_MessageDelivery_FetchPart] ON [transport].[MessageDelivery]
(
	[QueueId] ASC,
	[PartitionKey] ASC,
	[Priority] ASC,
	[EnqueueTime] ASC,
	[MessageDeliveryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_MessageDelivery_TransportMessageId]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_MessageDelivery_TransportMessageId] ON [transport].[MessageDelivery]
(
	[TransportMessageId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_Queue_AutoDelete]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_Queue_AutoDelete] ON [transport].[Queue]
(
	[AutoDelete] ASC
)
INCLUDE([Id]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_Queue_Name_Type]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_Queue_Name_Type] ON [transport].[Queue]
(
	[Name] ASC,
	[Type] ASC
)
INCLUDE([Id]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_QueueMetric_Unique]    Script Date: 19/11/2024 23:30:28 ******/
CREATE UNIQUE NONCLUSTERED INDEX [IX_QueueMetric_Unique] ON [transport].[QueueMetric]
(
	[StartTime] ASC,
	[Duration] ASC,
	[QueueId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_QueueSubscription_Destination]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_QueueSubscription_Destination] ON [transport].[QueueSubscription]
(
	[DestinationId] ASC
)
INCLUDE([Id],[SourceId],[SubType],[RoutingKey],[Filter]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_QueueSubscription_Source]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_QueueSubscription_Source] ON [transport].[QueueSubscription]
(
	[SourceId] ASC
)
INCLUDE([Id],[DestinationId],[SubType],[RoutingKey],[Filter]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_QueueSubscription_Unique]    Script Date: 19/11/2024 23:30:28 ******/
CREATE UNIQUE NONCLUSTERED INDEX [IX_QueueSubscription_Unique] ON [transport].[QueueSubscription]
(
	[SourceId] ASC,
	[DestinationId] ASC,
	[SubType] ASC,
	[RoutingKey] ASC,
	[Filter] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_Topic_Name]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_Topic_Name] ON [transport].[Topic]
(
	[Name] ASC
)
INCLUDE([Id]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_TopicSubscription_Destination]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_TopicSubscription_Destination] ON [transport].[TopicSubscription]
(
	[DestinationId] ASC
)
INCLUDE([Id],[SourceId],[SubType],[RoutingKey],[Filter]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_TopicSubscription_Source]    Script Date: 19/11/2024 23:30:28 ******/
CREATE NONCLUSTERED INDEX [IX_TopicSubscription_Source] ON [transport].[TopicSubscription]
(
	[SourceId] ASC
)
INCLUDE([Id],[DestinationId],[SubType],[RoutingKey],[Filter]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_TopicSubscription_Unique]    Script Date: 19/11/2024 23:30:28 ******/
CREATE UNIQUE NONCLUSTERED INDEX [IX_TopicSubscription_Unique] ON [transport].[TopicSubscription]
(
	[SourceId] ASC,
	[DestinationId] ASC,
	[SubType] ASC,
	[RoutingKey] ASC,
	[Filter] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [transport].[Message] ADD  DEFAULT (getutcdate()) FOR [SentTime]
GO
ALTER TABLE [transport].[MessageDelivery] ADD  DEFAULT (NEXT VALUE FOR [transport].[DeliverySequence]) FOR [MessageDeliveryId]
GO
ALTER TABLE [transport].[Queue] ADD  DEFAULT (NEXT VALUE FOR [transport].[TopologySequence]) FOR [Id]
GO
ALTER TABLE [transport].[Queue] ADD  DEFAULT (getutcdate()) FOR [Updated]
GO
ALTER TABLE [transport].[QueueSubscription] ADD  DEFAULT (NEXT VALUE FOR [transport].[TopologySequence]) FOR [Id]
GO
ALTER TABLE [transport].[QueueSubscription] ADD  DEFAULT (getutcdate()) FOR [Updated]
GO
ALTER TABLE [transport].[Topic] ADD  DEFAULT (NEXT VALUE FOR [transport].[TopologySequence]) FOR [Id]
GO
ALTER TABLE [transport].[Topic] ADD  DEFAULT (getutcdate()) FOR [Updated]
GO
ALTER TABLE [transport].[TopicSubscription] ADD  DEFAULT (NEXT VALUE FOR [transport].[TopologySequence]) FOR [Id]
GO
ALTER TABLE [transport].[TopicSubscription] ADD  DEFAULT (getutcdate()) FOR [Updated]
GO
ALTER TABLE [transport].[MessageDelivery]  WITH CHECK ADD FOREIGN KEY([TransportMessageId])
REFERENCES [transport].[Message] ([TransportMessageId])
ON DELETE CASCADE
GO
ALTER TABLE [transport].[QueueSubscription]  WITH CHECK ADD FOREIGN KEY([DestinationId])
REFERENCES [transport].[Queue] ([Id])
ON DELETE CASCADE
GO
ALTER TABLE [transport].[QueueSubscription]  WITH CHECK ADD FOREIGN KEY([SourceId])
REFERENCES [transport].[Topic] ([Id])
ON DELETE CASCADE
GO
ALTER TABLE [transport].[TopicSubscription]  WITH CHECK ADD FOREIGN KEY([DestinationId])
REFERENCES [transport].[Topic] ([Id])
GO
ALTER TABLE [transport].[TopicSubscription]  WITH CHECK ADD FOREIGN KEY([SourceId])
REFERENCES [transport].[Topic] ([Id])
GO
/****** Object:  StoredProcedure [transport].[CreateQueue]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[CreateQueue]
    @QueueName nvarchar(256),
    @AutoDelete integer = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @QueueName IS NULL OR LEN(@QueueName) < 1
    BEGIN
        THROW 50000, 'Queue name was null or empty', 1;
    END

    DECLARE @QueueTable table (Id BIGINT, Type tinyint)
    MERGE INTO transport.Queue WITH (ROWLOCK) AS target
        USING (VALUES
                   (@QueueName, 1, @AutoDelete),
                   (@QueueName, 2, @AutoDelete),
                   (@QueueName, 3, @AutoDelete)
               ) AS source (Name, Type, AutoDelete)
        ON (target.Name = source.Name AND target.Type = source.Type)
        WHEN MATCHED THEN UPDATE SET Updated = GETUTCDATE(), AutoDelete = COALESCE(source.AutoDelete, target.AutoDelete)
        WHEN NOT MATCHED THEN INSERT (Name, Type, AutoDelete)
        VALUES (source.Name, source.Type, source.AutoDelete)
        OUTPUT inserted.Id, inserted.Type INTO @QueueTable;

    SET NOCOUNT OFF
    SELECT TOP 1 Id FROM @QueueTable WHERE Type = 1;
END
GO
/****** Object:  StoredProcedure [transport].[CreateQueueSubscription]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[CreateQueueSubscription]
    @SourceTopicName nvarchar(256),
    @DestinationQueueName nvarchar(256),
    @SubscriptionType tinyint = 1,
    @RoutingKey varchar(256) = '',
    @Filter varchar(1024) = '{}'
AS
BEGIN
    SET NOCOUNT ON;

    IF @SourceTopicName IS NULL OR LEN(@SourceTopicName) < 1
    BEGIN
        THROW 50000, 'Source topic name was null or empty', 1;
    END

    IF @DestinationQueueName IS NULL OR LEN(@DestinationQueueName) < 1
    BEGIN
        THROW 50000, 'Destination queue name was null or empty', 1;
    END

    DECLARE @SourceTopicId BIGINT
    SELECT @SourceTopicId = t.Id FROM transport.Topic t WHERE t.Name = @SourceTopicName;
    IF @SourceTopicId IS NULL
    BEGIN
        THROW 50000, 'Destination topic name was null or empty', 1;
    END

    DECLARE @DestinationQueueId BIGINT
    SELECT @DestinationQueueId = q.Id FROM transport.Queue q WHERE q.Name = @DestinationQueueName AND q.Type = 1;
    IF @DestinationQueueId IS NULL
    BEGIN
        THROW 50000, 'Destination queue not found', 1;
    END

    DECLARE @ResultTable table (Id BIGINT)
    MERGE INTO transport.QueueSubscription WITH (ROWLOCK) AS target
        USING (VALUES (@SourceTopicId, @DestinationQueueId, @SubscriptionType, COALESCE(@RoutingKey, ''), COALESCE(@Filter, '{}')))
            AS source (SourceId, DestinationId, SubType, RoutingKey, Filter)
        ON (target.SourceId = source.SourceId AND target.DestinationId = source.DestinationId AND target.SubType = source.SubType
            AND target.RoutingKey = source.RoutingKey AND target.Filter = source.Filter)
        WHEN MATCHED THEN UPDATE SET Updated = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT (SourceId, DestinationId, SubType, RoutingKey, Filter)
        VALUES (source.SourceId, source.DestinationId, source.SubType, source.RoutingKey, source.Filter)
        OUTPUT inserted.Id INTO @ResultTable;

    SET NOCOUNT OFF
    SELECT TOP 1 Id FROM @ResultTable;
END;
GO
/****** Object:  StoredProcedure [transport].[CreateTopic]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[CreateTopic]
    @TopicName nvarchar(256)
AS
BEGIN
    SET NOCOUNT ON;

    IF @TopicName IS NULL OR LEN(@TopicName) < 1
    BEGIN
        THROW 50000, 'Topic name was null or empty', 1;
    END

    DECLARE @TopicTable table (Id BIGINT)
    MERGE INTO transport.Topic WITH (ROWLOCK) AS target
        USING (VALUES (@TopicName)) AS source (Name)
        ON (target.Name = source.Name)
        WHEN MATCHED THEN UPDATE SET Updated = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT (Name)
        VALUES (source.Name)
        OUTPUT inserted.Id INTO @TopicTable;

    SET NOCOUNT OFF
    SELECT TOP 1 Id FROM @TopicTable;
END;
GO
/****** Object:  StoredProcedure [transport].[CreateTopicSubscription]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[CreateTopicSubscription]
    @SourceTopicName nvarchar(256),
    @DestinationTopicName nvarchar(256),
    @SubscriptionType tinyint = 1,
    @RoutingKey varchar(256) = '',
    @Filter varchar(1024) = '{}'
AS
BEGIN
    SET NOCOUNT ON;

    IF @SourceTopicName IS NULL OR LEN(@SourceTopicName) < 1
    BEGIN
        THROW 50000, 'Source topic name was null or empty', 1;
    END

    IF @DestinationTopicName IS NULL OR LEN(@DestinationTopicName) < 1
    BEGIN
        THROW 50000, 'Destination topic name was null or empty', 1;
    END

    DECLARE @SourceTopicId BIGINT
    SELECT @SourceTopicId = t.Id FROM transport.Topic t WHERE t.Name = @SourceTopicName;
    IF @SourceTopicId IS NULL
    BEGIN
        THROW 50000, 'Source topic not found', 1;
    END

    DECLARE @DestinationTopicId BIGINT
    SELECT @DestinationTopicId = t.Id FROM transport.Topic t WHERE t.Name = @DestinationTopicName;
    IF @DestinationTopicId IS NULL
    BEGIN
        THROW 50000, 'Destination topic not found', 1;
    END

    DECLARE @ResultTable table (Id BIGINT)
    MERGE INTO transport.TopicSubscription WITH (ROWLOCK) AS target
        USING (VALUES (@SourceTopicId, @DestinationTopicId, @SubscriptionType, COALESCE(@RoutingKey, ''), COALESCE(@Filter, '{}')))
            AS source (SourceId, DestinationId, SubType, RoutingKey, Filter)
        ON (target.SourceId = source.SourceId AND target.DestinationId = source.DestinationId AND target.SubType = source.SubType
            AND target.RoutingKey = source.RoutingKey AND target.Filter = source.Filter)
        WHEN MATCHED THEN UPDATE SET Updated = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT (SourceId, DestinationId, SubType, RoutingKey, Filter)
        VALUES (source.SourceId, source.DestinationId, source.SubType, source.RoutingKey, source.Filter)
        OUTPUT inserted.Id INTO @ResultTable;

    SET NOCOUNT OFF
    SELECT TOP 1 Id FROM @ResultTable;
END;
GO
/****** Object:  StoredProcedure [transport].[DeleteMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[DeleteMessage]
    @messageDeliveryId bigint,
    @lockId uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @outMessageDeliveryId bigint;
    DECLARE @outTransportMessageId uniqueidentifier;
    DECLARE @outQueueId bigint;

    DECLARE @DeletedMessages TABLE (
        MessageDeliveryId bigint,
        TransportMessageId uniqueidentifier,
        QueueId bigint
    );

    DELETE
    FROM transport.MessageDelivery
    OUTPUT deleted.MessageDeliveryId, deleted.TransportMessageId, deleted.QueueId
    INTO @DeletedMessages
    WHERE MessageDeliveryId = @messageDeliveryId
    AND LockId = @lockId;

    SELECT TOP 1 @outMessageDeliveryId = MessageDeliveryId, @outTransportMessageId = TransportMessageId, @outQueueId = QueueId
        FROM @DeletedMessages;

    IF @outTransportMessageId IS NOT NULL
    BEGIN
        DELETE m
        FROM transport.Message m
        WHERE m.TransportMessageId = @outTransportMessageId
        AND NOT EXISTS (SELECT 1 FROM transport.MessageDelivery md WHERE md.TransportMessageId = @outTransportMessageId);

        INSERT INTO transport.QueueMetricCapture (Captured, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
            VALUES (GETUTCDATE(), @outQueueId, 1, 0, 0);
    END;

    RETURN @outMessageDeliveryId;
END
GO
/****** Object:  StoredProcedure [transport].[DeleteScheduledMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[DeleteScheduledMessage]
    @tokenId uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DeletedMessages TABLE (
        TransportMessageId uniqueidentifier
    );

    DELETE m
    OUTPUT deleted.TransportMessageId
    INTO @DeletedMessages (TransportMessageId)
    FROM transport.Message m
    LEFT JOIN transport.MessageDelivery md ON md.TransportMessageId = m.TransportMessageId
    WHERE m.SchedulingTokenId = @tokenId
    AND md.DeliveryCount = 0
    AND md.LockId IS NULL;

    SELECT TransportMessageId
        FROM @DeletedMessages;
END
GO
/****** Object:  StoredProcedure [transport].[FetchMessages]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[FetchMessages]
    @queueName varchar(256),
    @consumerId uniqueidentifier,
    @lockId uniqueidentifier,
    @lockDuration int,
    @fetchCount int = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @queueId bigint;
    DECLARE @enqueueTime datetime2;
    DECLARE @now datetime2;

    SELECT @queueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @queueName AND q.Type = 1;

    IF @queueId IS NULL
    BEGIN
        THROW 50000, 'Queue not found', 1;
    END;

    IF @lockDuration <= 0
    BEGIN
        THROW 50000, 'Invalid lock duration', 1;
    END;

    SET @now = SYSUTCDATETIME();
    SET @enqueueTime = DATEADD(SECOND, @lockDuration, @now);

    DECLARE @ResultTable TABLE (
        TransportMessageId uniqueidentifier,
        QueueId bigint,
        Priority smallint,
        MessageDeliveryId bigint,
        ConsumerId uniqueidentifier,
        LockId uniqueidentifier,
        EnqueueTime datetime2,
        ExpirationTime datetime2,
        DeliveryCount int,
        PartitionKey text,
        RoutingKey text,
        TransportHeaders nvarchar(max),
        ContentType text,
        MessageType text,
        Body nvarchar(max),
        BinaryBody varbinary(max),
        MessageId uniqueidentifier,
        CorrelationId uniqueidentifier,
        ConversationId uniqueidentifier,
        RequestId uniqueidentifier,
        InitiatorId uniqueidentifier,
        SourceAddress text,
        DestinationAddress text,
        ResponseAddress text,
        FaultAddress text,
        SentTime datetime2,
        Headers nvarchar(max),
        Host nvarchar(max)
    );

    WITH msgs AS (
        SELECT
            md.*
        FROM
            transport.MessageDelivery md WITH (ROWLOCK, READPAST, UPDLOCK)
        WHERE
            md.QueueId = @queueId
            AND md.EnqueueTime <= @now
            AND md.DeliveryCount < md.MaxDeliveryCount
        ORDER BY
            md.Priority ASC,
            md.EnqueueTime ASC,
            md.MessageDeliveryId ASC
        OFFSET 0 ROWS
        FETCH NEXT @fetchCount ROWS ONLY
    )
    UPDATE dm
    SET
        DeliveryCount = dm.DeliveryCount + 1,
        LastDelivered = @now,
        ConsumerId = @consumerId,
        LockId = @lockId,
        EnqueueTime = @enqueueTime
    OUTPUT
        inserted.TransportMessageId,
        inserted.QueueId,
        inserted.Priority,
        inserted.MessageDeliveryId,
        inserted.ConsumerId,
        inserted.LockId,
        inserted.EnqueueTime,
        inserted.ExpirationTime,
        inserted.DeliveryCount,
        inserted.PartitionKey,
        inserted.RoutingKey,
        inserted.TransportHeaders,
        m.ContentType,
        m.MessageType,
        m.Body,
        m.BinaryBody,
        m.MessageId,
        m.CorrelationId,
        m.ConversationId,
        m.RequestId,
        m.InitiatorId,
        m.SourceAddress,
        m.DestinationAddress,
        m.ResponseAddress,
        m.FaultAddress,
        m.SentTime,
        m.Headers,
        m.Host
    INTO
        @ResultTable (
            TransportMessageId ,
            QueueId ,
            Priority ,
            MessageDeliveryId ,
            ConsumerId ,
            LockId ,
            EnqueueTime ,
            ExpirationTime ,
            DeliveryCount ,
            PartitionKey ,
            RoutingKey ,
            TransportHeaders,
            ContentType ,
            MessageType ,
            Body,
            BinaryBody,
            MessageId ,
            CorrelationId ,
            ConversationId ,
            RequestId ,
            InitiatorId ,
            SourceAddress ,
            DestinationAddress ,
            ResponseAddress ,
            FaultAddress ,
            SentTime ,
            Headers,
            Host
        )
    FROM
        transport.MessageDelivery dm
        INNER JOIN msgs ON dm.MessageDeliveryId = msgs.MessageDeliveryId
        INNER JOIN transport.Message m ON msgs.TransportMessageId = m.TransportMessageId;

    SELECT * FROM @ResultTable;
END
GO
/****** Object:  StoredProcedure [transport].[FetchMessagesPartitioned]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[FetchMessagesPartitioned]
    @queueName varchar(256),
    @consumerId uniqueidentifier,
    @lockId uniqueidentifier,
    @lockDuration int,
    @fetchCount int = 1,
    @concurrentCount int = 1,
    @ordered int = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @queueId bigint;
    DECLARE @enqueueTime datetime2;
    DECLARE @now datetime2;

    SELECT @queueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @queueName AND q.Type = 1;

    IF @queueId IS NULL
    BEGIN
        THROW 50000, 'Queue not found', 1;
    END;

    IF @lockDuration <= 0
    BEGIN
        THROW 50000, 'Invalid lock duration', 1;
    END;

    SET @now = SYSUTCDATETIME();
    SET @enqueueTime = DATEADD(SECOND, @lockDuration, @now);

    DECLARE @ResultTable TABLE (
        TransportMessageId uniqueidentifier,
        QueueId bigint,
        Priority smallint,
        MessageDeliveryId bigint,
        ConsumerId uniqueidentifier,
        LockId uniqueidentifier,
        EnqueueTime datetime2,
        ExpirationTime datetime2,
        DeliveryCount int,
        PartitionKey text,
        RoutingKey text,
        TransportHeaders nvarchar(max),
        ContentType text,
        MessageType text,
        Body nvarchar(max),
        BinaryBody varbinary(max),
        MessageId uniqueidentifier,
        CorrelationId uniqueidentifier,
        ConversationId uniqueidentifier,
        RequestId uniqueidentifier,
        InitiatorId uniqueidentifier,
        SourceAddress text,
        DestinationAddress text,
        ResponseAddress text,
        FaultAddress text,
        SentTime datetime2,
        Headers nvarchar(max),
        Host nvarchar(max)
    );

    WITH ready AS (SELECT mdx.MessageDeliveryId,
                          mdx.EnqueueTime,
                          mdx.LockId,
                          mdx.Priority,
                          row_number() over (partition by mdx.PartitionKey order by mdx.Priority, mdx.EnqueueTime, mdx.MessageDeliveryId) as row_normal,
                          row_number() over (partition by mdx.PartitionKey order by mdx.Priority, mdx.MessageDeliveryId, mdx.EnqueueTime) as row_ordered,
                          first_value(CASE WHEN mdx.EnqueueTime > @now THEN mdx.ConsumerId END) over (partition by mdx.PartitionKey
                           order by mdx.EnqueueTime DESC, mdx.MessageDeliveryId DESC) as ConsumerId,
                          sum(CASE WHEN mdx.EnqueueTime > @now AND mdx.ConsumerId = @consumerId AND mdx.LockId IS NOT NULL THEN 1 END)
                           over (partition by mdx.PartitionKey
                               order by mdx.EnqueueTime DESC, mdx.MessageDeliveryId DESC) as ActiveCount
                   FROM transport.MessageDelivery mdx WITH (ROWLOCK, READPAST, UPDLOCK)
                   WHERE mdx.QueueId = @queueId
                     AND mdx.DeliveryCount < mdx.MaxDeliveryCount),
         so_ready as (SELECT ready.MessageDeliveryId
                      FROM ready
                      WHERE ( ( @ordered = 0 AND ready.row_normal <= @concurrentCount) OR ( @ordered = 1 AND ready.row_ordered <= @concurrentCount ) )
                        AND (ready.ConsumerId IS NULL OR ready.ConsumerId = @consumerId)
                        AND (ActiveCount < @concurrentCount OR ActiveCount IS NULL)
                        AND ready.EnqueueTime <= @now
                      ORDER BY ready.Priority, ready.EnqueueTime, ready.MessageDeliveryId
                      OFFSET 0 ROWS FETCH NEXT @fetchCount ROWS ONLY),
         msgs AS (SELECT md.*
                  FROM transport.MessageDelivery md
                  WITH (ROWLOCK, READPAST, UPDLOCK)
                  WHERE md.MessageDeliveryId IN (SELECT MessageDeliveryId FROM so_ready))
    UPDATE dm
    SET
        DeliveryCount = dm.DeliveryCount + 1,
        LastDelivered = @now,
        ConsumerId = @consumerId,
        LockId = @lockId,
        EnqueueTime = @enqueueTime
    OUTPUT
        inserted.TransportMessageId,
        inserted.QueueId,
        inserted.Priority,
        inserted.MessageDeliveryId,
        inserted.ConsumerId,
        inserted.LockId,
        inserted.EnqueueTime,
        inserted.ExpirationTime,
        inserted.DeliveryCount,
        inserted.PartitionKey,
        inserted.RoutingKey,
        inserted.TransportHeaders,
        m.ContentType,
        m.MessageType,
        m.Body,
        m.BinaryBody,
        m.MessageId,
        m.CorrelationId,
        m.ConversationId,
        m.RequestId,
        m.InitiatorId,
        m.SourceAddress,
        m.DestinationAddress,
        m.ResponseAddress,
        m.FaultAddress,
        m.SentTime,
        m.Headers,
        m.Host
    INTO
        @ResultTable (
            TransportMessageId ,
            QueueId ,
            Priority ,
            MessageDeliveryId ,
            ConsumerId ,
            LockId ,
            EnqueueTime ,
            ExpirationTime ,
            DeliveryCount ,
            PartitionKey ,
            RoutingKey ,
            TransportHeaders,
            ContentType ,
            MessageType ,
            Body,
            BinaryBody,
            MessageId ,
            CorrelationId ,
            ConversationId ,
            RequestId ,
            InitiatorId ,
            SourceAddress ,
            DestinationAddress ,
            ResponseAddress ,
            FaultAddress ,
            SentTime ,
            Headers,
            Host
        )
    FROM
        transport.MessageDelivery dm
        INNER JOIN msgs ON dm.MessageDeliveryId = msgs.MessageDeliveryId
        INNER JOIN transport.Message m ON msgs.TransportMessageId = m.TransportMessageId;

    SELECT * FROM @ResultTable;
END
GO
/****** Object:  StoredProcedure [transport].[MoveMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[MoveMessage]
    @messageDeliveryId bigint,
    @lockId uniqueidentifier,
    @queueName nvarchar(256),
    @queueType int,
    @headers nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @queueId bigint
    SELECT @queueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @queueName AND q.Type = @queueType;

    IF @queueId IS NULL
    BEGIN
        THROW 50000, 'Queue not found', 1;
    END;

    DECLARE @updatedMessages TABLE (
        MessageDeliveryId bigint,
        QueueId bigint
    );

    UPDATE md
    SET EnqueueTime = SYSUTCDATETIME(), QueueId = @queueId, LockId = NULL, ConsumerId = NULL, TransportHeaders = @headers
    OUTPUT inserted.MessageDeliveryId, inserted.QueueId INTO @updatedMessages
    FROM transport.MessageDelivery md
    WHERE md.MessageDeliveryId = @messageDeliveryId AND md.LockId = @lockId;

    DECLARE @outQueueId bigint
    SELECT TOP 1 @outQueueId = QueueID FROM @updatedMessages;

    IF @outQueueId IS NOT NULL
    BEGIN
        INSERT INTO transport.QueueMetricCapture (Captured, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
            VALUES (GETUTCDATE(), @outQueueId, 0, CASE WHEN @queueType = 2 THEN 1 ELSE 0 END, CASE WHEN @queueType = 3 THEN 1 ELSE 0 END);
    END;

    SELECT MessageDeliveryId FROM @updatedMessages;
END
GO
/****** Object:  StoredProcedure [transport].[ProcessMetrics]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[ProcessMetrics]
    @rowLimit int
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DeletedMetrics TABLE
                            (
                                StartTime       datetime2 not null,
                                Duration        int       not null,
                                QueueId         bigint    not null,
                                ConsumeCount    bigint    not null,
                                ErrorCount      bigint    not null,
                                DeadLetterCount bigint    not null
                            );

    DELETE
    FROM transport.QueueMetricCapture
    OUTPUT CONVERT(DATETIME, CONVERT(VARCHAR(16), deleted.Captured, 120) + ':00'), 60, deleted.QueueId,
                                                                                       deleted.ConsumeCount,
                                                                                       deleted.ErrorCount,
                                                                                       deleted.DeadLetterCount
        INTO @DeletedMetrics
    WHERE Id < COALESCE((SELECT MIN(id) FROM transport.queuemetriccapture), 0) + @rowLimit;

    MERGE INTO transport.QueueMetric WITH (ROWLOCK) AS target
    USING (SELECT m.StartTime,
                  m.Duration,
                  m.QueueId,
                  sum(m.ConsumeCount),
                  sum(m.ErrorCount),
                  sum(m.DeadLetterCount)
           FROM @DeletedMetrics m
           GROUP BY StartTime, m.Duration, m.QueueId) as source
        (StartTime, Duration, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
    ON target.StartTime = source.starttime AND target.Duration = source.duration AND
       target.QueueId = source.QueueId
    WHEN MATCHED THEN
        UPDATE
        SET ConsumeCount    = source.ConsumeCount + target.ConsumeCount,
            ErrorCount      = source.ErrorCount + target.ErrorCount,
            DeadLetterCount = source.DeadLetterCount + target.DeadLetterCount
    WHEN NOT MATCHED THEN
        INSERT (starttime, duration, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
        values (source.starttime, source.duration, source.QueueId, source.ConsumeCount, source.ErrorCount,
                source.DeadLetterCount);

    DELETE
    FROM @DeletedMetrics;

    DELETE
    FROM transport.QueueMetric
    OUTPUT CONVERT(DATETIME, CONVERT(VARCHAR(13), deleted.StartTime, 120) + ':00:00'), 3600, deleted.QueueId,
                                                                                             deleted.ConsumeCount,
                                                                                             deleted.ErrorCount,
                                                                                             deleted.DeadLetterCount
        INTO @DeletedMetrics
    WHERE Duration = 60
      AND StartTime < DATEADD(HOUR, -8, GETUTCDATE())

    MERGE INTO transport.QueueMetric WITH (ROWLOCK) AS target
    USING (SELECT m.StartTime,
                  m.Duration,
                  m.QueueId,
                  sum(m.ConsumeCount),
                  sum(m.ErrorCount),
                  sum(m.DeadLetterCount)
           FROM @DeletedMetrics m
           GROUP BY StartTime, m.Duration, m.QueueId) as source
        (StartTime, Duration, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
    ON target.StartTime = source.starttime AND target.Duration = source.duration AND
       target.QueueId = source.QueueId
    WHEN MATCHED THEN
        UPDATE
        SET ConsumeCount    = source.ConsumeCount + target.ConsumeCount,
            ErrorCount      = source.ErrorCount + target.ErrorCount,
            DeadLetterCount = source.DeadLetterCount + target.DeadLetterCount
    WHEN NOT MATCHED THEN
        INSERT (starttime, duration, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
        values (source.starttime, source.duration, source.QueueId, source.ConsumeCount, source.ErrorCount,
                source.DeadLetterCount);

    DELETE
    FROM @DeletedMetrics;

    DELETE
    FROM transport.QueueMetric
    OUTPUT CONVERT(DATETIME, CONVERT(VARCHAR(10), deleted.StartTime, 120)), 86400, deleted.QueueId,
                                                                                   deleted.ConsumeCount,
                                                                                   deleted.ErrorCount,
                                                                                   deleted.DeadLetterCount
        INTO @DeletedMetrics
    WHERE Duration = 3600
      AND StartTime < DATEADD(HOUR, -48, GETUTCDATE())

    MERGE INTO transport.QueueMetric WITH (ROWLOCK) AS target
    USING (SELECT m.StartTime,
                  m.Duration,
                  m.QueueId,
                  sum(m.ConsumeCount),
                  sum(m.ErrorCount),
                  sum(m.DeadLetterCount)
           FROM @DeletedMetrics m
           GROUP BY StartTime, m.Duration, m.QueueId) as source
        (StartTime, Duration, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
    ON target.StartTime = source.starttime AND target.Duration = source.duration AND
       target.QueueId = source.QueueId
    WHEN MATCHED THEN
        UPDATE
        SET ConsumeCount    = source.ConsumeCount + target.ConsumeCount,
            ErrorCount      = source.ErrorCount + target.ErrorCount,
            DeadLetterCount = source.DeadLetterCount + target.DeadLetterCount
    WHEN NOT MATCHED THEN
        INSERT (starttime, duration, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
        values (source.starttime, source.duration, source.QueueId, source.ConsumeCount, source.ErrorCount,
                source.DeadLetterCount);

    DELETE
    FROM transport.QueueMetric
    WHERE StartTime < DATEADD(DAY, -90, GETUTCDATE());
END
GO
/****** Object:  StoredProcedure [transport].[PublishMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[PublishMessage]
    @entityName varchar(256),
    @priority int = 100,
    @transportMessageId uniqueidentifier,
    @body nvarchar(max) = NULL,
    @binaryBody varbinary(max) = NULL,
    @contentType varchar(max) = NULL,
    @messageType varchar(max) = NULL,
    @messageId uniqueidentifier = NULL,
    @correlationId uniqueidentifier = NULL,
    @conversationId uniqueidentifier = NULL,
    @requestId uniqueidentifier = NULL,
    @initiatorId uniqueidentifier = NULL,
    @sourceAddress varchar(max) = NULL,
    @destinationAddress varchar(max) = NULL,
    @responseAddress varchar(max) = NULL,
    @faultAddress varchar(max) = NULL,
    @sentTime datetimeoffset = NULL,
    @headers nvarchar(max) = NULL,
    @host nvarchar(max) = NULL,
    @partitionKey nvarchar(128) = NULL,
    @routingKey nvarchar(256) = NULL,
    @delay int = 0,
    @schedulingTokenId uniqueidentifier = NULL,
    @maxDeliveryCount int = 10
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @vTopicId bigint;
    DECLARE @vRowCount bigint;
    DECLARE @vEnqueueTime datetimeoffset;
    DECLARE @vRow table (
        queueId bigint,
        transportMessageId uniqueidentifier,
        priority int,
        enqueueTime datetimeoffset,
        routingKey varchar(100)
    );

    IF @entityName IS NULL OR LEN(@entityName) < 1
    BEGIN
        THROW 50000, 'Topic names must not be null or empty', 1;
    END;

    SELECT @vTopicId = t.Id
    FROM transport.Topic t
    WHERE t.Name = @entityName;

    IF @vTopicId IS NULL
    BEGIN
        THROW 50000, 'Topic not found', 1;
    END;

    SET @vEnqueueTime = GETUTCDATE();

    IF @delay > 0
    BEGIN
        SET @vEnqueueTime = DATEADD(SECOND, @delay, @vEnqueueTime);
    END;

    INSERT INTO transport.Message (
        TransportMessageId, Body, BinaryBody, ContentType, MessageType, MessageId,
        CorrelationId, ConversationId, RequestId, InitiatorId,
        SourceAddress, DestinationAddress, ResponseAddress, FaultAddress,
        SentTime, Headers, Host, SchedulingTokenId
    )
    VALUES (
        @transportMessageId, @body, @binaryBody, @contentType, @messageType, @messageId,
        @correlationId, @conversationId, @requestId, @initiatorId,
        @sourceAddress, @destinationAddress, @responseAddress, @faultAddress,
        @sentTime, @headers, @host, @schedulingTokenId
    );

    ;WITH Fabric AS (
        SELECT ts.SourceId, ts.DestinationId
        FROM transport.Topic t
        LEFT JOIN transport.TopicSubscription ts ON t.Id = ts.SourceId
            AND (
                (ts.SubType = 1)
                OR (ts.SubType = 2 AND @routingKey = ts.RoutingKey)
                OR (ts.SubType = 3 AND @routingKey LIKE ts.RoutingKey)
            )
        WHERE t.Id = @vTopicId

        UNION ALL

        SELECT ts.SourceId, ts.DestinationId
        FROM transport.TopicSubscription ts
        JOIN Fabric ON ts.SourceId = fabric.DestinationId
        WHERE
            (ts.SubType = 1)
            OR (ts.SubType = 2 AND @routingKey = ts.RoutingKey)
            OR (ts.SubType = 3 AND @routingKey LIKE ts.RoutingKey)
    )
    INSERT INTO transport.MessageDelivery (QueueId, TransportMessageId, Priority, EnqueueTime, DeliveryCount, MaxDeliveryCount, PartitionKey, RoutingKey)
    OUTPUT inserted.QueueId, inserted.TransportMessageId, inserted.Priority, inserted.EnqueueTime, inserted.RoutingKey INTO @vRow
    SELECT DISTINCT qs.DestinationId, @transportMessageId, @priority, @vEnqueueTime, 0, @maxDeliveryCount, @partitionKey, @routingKey
    FROM transport.QueueSubscription qs
    JOIN Fabric ON (qs.SourceId = fabric.DestinationId OR qs.SourceId = @vTopicId)
        AND (  (qs.SubType = 1)
            OR (qs.SubType = 2 AND @routingKey = qs.RoutingKey)
            OR (qs.SubType = 3 AND @routingKey LIKE qs.RoutingKey));

    SELECT @vRowCount = COUNT(*) FROM @vRow;

    IF @vRowCount = 0
    BEGIN
        DELETE FROM transport.Message WHERE TransportMessageId = @transportMessageId;
    END;

    RETURN @vRowCount;
END;
GO
/****** Object:  StoredProcedure [transport].[PurgeQueue]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[PurgeQueue]
    @queueName varchar(256)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DeletedMessages TABLE (
        TransportMessageId uniqueidentifier INDEX DMIDX CLUSTERED
    );

    DELETE FROM transport.MessageDelivery
        OUTPUT deleted.TransportMessageId
        INTO @DeletedMessages
        FROM transport.MessageDelivery mdx
            INNER JOIN transport.queue q on mdx.queueid = q.Id
        WHERE q.name = @queueName

    DELETE FROM transport.Message
        FROM transport.Message m
            INNER JOIN @DeletedMessages dm ON m.TransportMessageId = dm.TransportMessageId
        WHERE NOT EXISTS (SELECT 1 FROM transport.MessageDelivery md WHERE md.TransportMessageId = m.TransportMessageId);

    SELECT COUNT(*) FROM @DeletedMessages
END
GO
/****** Object:  StoredProcedure [transport].[PurgeTopology]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[PurgeTopology]
AS
BEGIN
    WITH expired AS (SELECT q.Id, q.name, DATEADD(second, -q.autodelete, GETUTCDATE()) as expires_at
                     FROM transport.Queue q
                     WHERE q.Type = 1 AND  q.AutoDelete IS NOT NULL AND DATEADD(second, -q.AutoDelete, GETUTCDATE()) > Updated),
         metrics AS (SELECT qm.queueid, MAX(starttime) as start_time
                     FROM transport.queuemetric qm
                              INNER JOIN expired q2 on q2.Id = qm.QueueId
                     WHERE DATEADD(second, duration, starttime) > q2.expires_at
                     GROUP BY qm.queueid)
    DELETE FROM transport.Queue
    FROM transport.Queue qd
    INNER JOIN expired qdx ON qdx.Name = qd.Name
        WHERE qdx.Id NOT IN (SELECT QueueId FROM metrics);

END
GO
/****** Object:  StoredProcedure [transport].[RenewMessageLock]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[RenewMessageLock]
    @messageDeliveryId bigint,
    @lockId uniqueidentifier,
    @duration int
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @enqueueTime datetime2;
    SET @enqueueTime = DATEADD(SECOND, @duration, SYSUTCDATETIME());

    DECLARE @updatedMessages TABLE (
        MessageDeliveryId bigint,
        QueueId bigint
    );

    UPDATE md
    SET EnqueueTime = @enqueueTime
    OUTPUT inserted.MessageDeliveryId, inserted.QueueId INTO @updatedMessages
    FROM transport.MessageDelivery md
    WHERE md.MessageDeliveryId = @messageDeliveryId AND md.LockId = @lockId;

    DECLARE @queueId bigint
    SELECT TOP 1 @queueId = QueueID FROM @updatedMessages;

    IF @queueId IS NOT NULL
    BEGIN
        INSERT INTO transport.QueueMetricCapture (Captured, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
            VALUES (GETUTCDATE(), @queueId, 0, 0, 0);
    END;

    SELECT MessageDeliveryId FROM @updatedMessages;
END
GO
/****** Object:  StoredProcedure [transport].[RequeueMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[RequeueMessage] @messageDeliveryId bigint,
                                                   @targetQueueType int,
                                                   @delay int = 0,
                                                   @redeliveryCount int = 10
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT @targetQueueType BETWEEN 1 AND 3
        BEGIN
            THROW 50000, 'Invalid target queue type', 1;
        END;

    DECLARE @sourceQueueId bigint;
    SELECT @sourceQueueId = md.QueueId
    FROM transport.MessageDelivery md
    WHERE md.MessageDeliveryId = @messageDeliveryId;

    IF @sourceQueueId IS NULL
        BEGIN
            THROW 50000, 'Message delivery not found', 1;
        END;

    DECLARE @sourceQueueName nvarchar(256);
    DECLARE @sourceQueueType int;
    SELECT @sourceQueueName = q.Name, @sourceQueueType = q.Type
    FROM transport.Queue q
    WHERE q.Id = @sourceQueueId;

    IF @sourceQueueName IS NULL
        BEGIN
            THROW 50000, 'Queue not found', 1;
        END;

    IF @sourceQueueType = @targetQueueType
        BEGIN
            THROW 50000, 'Source and target queue type must not be the same', 1;
        END;

    DECLARE @targetQueueId bigint;
    SELECT @targetQueueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @sourceQueueName
      AND q.Type = @targetQueueType;

    IF @targetQueueId IS NULL
        BEGIN
            THROW 50000, 'Queue type not found', 1;
        END;

    DECLARE @enqueueTime datetime2;
    SET @enqueueTime = DATEADD(SECOND, @delay, SYSUTCDATETIME());

    UPDATE transport.MessageDelivery
    SET EnqueueTime      = @enqueueTime,
        QueueId          = @targetQueueId,
        MaxDeliveryCount = MessageDelivery.DeliveryCount + @redeliveryCount
    FROM (SELECT mdx.MessageDeliveryId
          FROM transport.MessageDelivery mdx WITH (ROWLOCK, UPDLOCK)
          WHERE mdx.QueueId = @sourceQueueId
            AND mdx.LockId IS NULL
            AND mdx.ConsumerId IS NULL
            AND (mdx.ExpirationTime IS NULL OR mdx.ExpirationTime > @enqueueTime)
            AND mdx.MessageDeliveryId = @messageDeliveryId) mdy
    WHERE mdy.MessageDeliveryId = MessageDelivery.MessageDeliveryId;

    RETURN @@ROWCOUNT;
END
GO
/****** Object:  StoredProcedure [transport].[RequeueMessages]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[RequeueMessages]
    @queueName nvarchar(256),
    @sourceQueueType int,
    @targetQueueType int,
    @messageCount int,
    @delay int = 0,
    @redeliveryCount int = 10
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT @sourceQueueType BETWEEN 1 AND 3
    BEGIN
        THROW 50000, 'Invalid source queue type', 1;
    END;

    IF NOT @targetQueueType BETWEEN 1 AND 3
    BEGIN
        THROW 50000, 'Invalid target queue type', 1;
    END;

    IF @sourceQueueType = @targetQueueType
    BEGIN
        THROW 50000, 'Source and target queue type must not be the same', 1;
    END;

    DECLARE @sourceQueueId bigint
    SELECT @sourceQueueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @queueName AND q.Type = @sourceQueueType;

    IF @sourceQueueId IS NULL
    BEGIN
        THROW 50000, 'Source queue not found', 1;
    END;

    DECLARE @targetQueueId bigint
    SELECT @targetQueueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @queueName AND q.Type = @targetQueueType;

    IF @targetQueueId IS NULL
    BEGIN
        THROW 50000, 'Target queue not found', 1;
    END;

    DECLARE @enqueueTime datetime2;
    SET @enqueueTime = DATEADD(SECOND, @delay, SYSUTCDATETIME());

    UPDATE transport.MessageDelivery
    SET EnqueueTime      = @enqueueTime,
        QueueId          = @targetQueueId,
        MaxDeliveryCount = MessageDelivery.DeliveryCount + @redeliveryCount
    FROM (SELECT mdx.MessageDeliveryId
          FROM transport.MessageDelivery mdx WITH (ROWLOCK, UPDLOCK)
          WHERE mdx.QueueId = @sourceQueueId
            AND mdx.LockId IS NULL
            AND mdx.ConsumerId IS NULL
            AND (mdx.ExpirationTime IS NULL OR mdx.ExpirationTime > @enqueueTime)
            ORDER BY mdx.MessageDeliveryId OFFSET 0 ROWS
        FETCH NEXT @messageCount ROWS ONLY) mdy
    WHERE mdy.MessageDeliveryId = MessageDelivery.MessageDeliveryId;

    RETURN @@ROWCOUNT
END
GO
/****** Object:  StoredProcedure [transport].[SendMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[SendMessage]
    @entityName varchar(256),
    @priority int = 100,
    @transportMessageId uniqueidentifier,
    @body nvarchar(max) = NULL,
    @binaryBody varbinary(max) = NULL,
    @contentType varchar(max) = NULL,
    @messageType varchar(max) = NULL,
    @messageId uniqueidentifier = NULL,
    @correlationId uniqueidentifier = NULL,
    @conversationId uniqueidentifier = NULL,
    @requestId uniqueidentifier = NULL,
    @initiatorId uniqueidentifier = NULL,
    @sourceAddress varchar(max) = NULL,
    @destinationAddress varchar(max) = NULL,
    @responseAddress varchar(max) = NULL,
    @faultAddress varchar(max) = NULL,
    @sentTime datetimeoffset = NULL,
    @headers nvarchar(max) = NULL,
    @host nvarchar(max) = NULL,
    @partitionKey nvarchar(128) = NULL,
    @routingKey nvarchar(256) = NULL,
    @delay int = 0,
    @schedulingTokenId uniqueidentifier = NULL,
    @maxDeliveryCount int = 10
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @vQueueId int;
    DECLARE @vEnqueueTime datetimeoffset;

    IF @entityName IS NULL OR LEN(@entityName) < 1
    BEGIN
        THROW 50000, 'Queue names must not be null or empty', 1;
    END;

    SELECT @vQueueId = q.Id FROM transport.Queue q WHERE q.Name = @entityName AND q.type = 1;
    IF @vQueueId IS NULL
    BEGIN
        THROW 50000, 'Queue not found', 1;
    END;

    SET @vEnqueueTime = GETUTCDATE();

    IF @delay > 0
    BEGIN
        SET @vEnqueueTime = DATEADD(SECOND, @delay, @vEnqueueTime);
    END;

    INSERT INTO transport.Message (
        TransportMessageId, Body, BinaryBody, ContentType, MessageType, MessageId,
        CorrelationId, ConversationId, RequestId, InitiatorId,
        SourceAddress, DestinationAddress, ResponseAddress, FaultAddress,
        SentTime, Headers, Host, SchedulingTokenId
    )
    VALUES (
        @transportMessageId, @body, @binaryBody, @contentType, @messageType, @messageId,
        @correlationId, @conversationId, @requestId, @initiatorId,
        @sourceAddress, @destinationAddress, @responseAddress, @faultAddress,
        @sentTime, @headers, @host, @schedulingTokenId
    );

    INSERT INTO transport.MessageDelivery (QueueId, TransportMessageId, Priority, EnqueueTime, DeliveryCount, MaxDeliveryCount, PartitionKey, RoutingKey)
    VALUES (@vQueueId, @transportMessageId, @priority, @vEnqueueTime, 0, @maxDeliveryCount, @partitionKey, @routingKey)

    RETURN 1;
END;
GO
/****** Object:  StoredProcedure [transport].[TouchQueue]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[TouchQueue]
    @queueName varchar(256)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @queueId bigint
    SELECT @queueId = q.Id
    FROM transport.Queue q
    WHERE q.Name = @queueName AND q.Type = 1;

    IF @queueId IS NULL
    BEGIN
        THROW 50000, 'Queue not found', 1;
    END;

    INSERT INTO transport.QueueMetricCapture (Captured, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
        VALUES (GETUTCDATE(), @queueId, 0, 0, 0);

END
GO
/****** Object:  StoredProcedure [transport].[UnlockMessage]    Script Date: 19/11/2024 23:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [transport].[UnlockMessage]
    @messageDeliveryId bigint,
    @lockId uniqueidentifier,
    @delay int,
    @headers nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @enqueueTime datetime2;
    SET @enqueueTime = DATEADD(SECOND, @delay, SYSUTCDATETIME());

    DECLARE @updatedMessages TABLE (
        MessageDeliveryId bigint,
        QueueId bigint
    );

    UPDATE md
    SET EnqueueTime = @enqueueTime, LockId = NULL, ConsumerId = NULL, TransportHeaders = @headers
    OUTPUT inserted.MessageDeliveryId, inserted.QueueId INTO @updatedMessages
    FROM transport.MessageDelivery md
    WHERE md.MessageDeliveryId = @messageDeliveryId AND md.LockId = @lockId;

    DECLARE @queueId bigint
    SELECT TOP 1 @queueId = QueueID FROM @updatedMessages;

    IF @queueId IS NOT NULL
    BEGIN
        INSERT INTO transport.QueueMetricCapture (Captured, QueueId, ConsumeCount, ErrorCount, DeadLetterCount)
            VALUES (GETUTCDATE(), @queueId, 0, 0, 0);
    END;

    SELECT MessageDeliveryId FROM @updatedMessages;
END
GO
USE [master]
GO
ALTER DATABASE [masstransitdb] SET  READ_WRITE 
GO
