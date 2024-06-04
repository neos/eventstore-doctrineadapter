<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use DateTimeImmutable;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Helper\BatchEventStream;
use Neos\EventStore\Model\Event;
use Neos\EventStore\Model\Event\CausationId;
use Neos\EventStore\Model\Event\CorrelationId;
use Neos\EventStore\Model\Event\EventId;
use Neos\EventStore\Model\Event\EventType;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\Event\Version;
use Neos\EventStore\Model\Events;
use Neos\EventStore\Model\EventStore\CommitResult;
use Neos\EventStore\Model\EventStore\Status;
use Neos\EventStore\Model\EventStream\EventStreamFilter;
use Neos\EventStore\Model\EventStream\EventStreamInterface;
use Neos\EventStore\Model\EventStream\ExpectedVersion;
use Neos\EventStore\Model\EventStream\MaybeVersion;
use Neos\EventStore\Model\EventStream\VirtualStreamName;
use Neos\EventStore\Model\EventStream\VirtualStreamType;
use Psr\Clock\ClockInterface;

final class DoctrineEventStore implements EventStoreInterface
{
    private readonly ClockInterface $clock;

    public function __construct(
        private readonly Connection $connection,
        private readonly string $eventTableName,
        ClockInterface $clock = null
    ) {
        $this->clock = $clock ?? new class implements ClockInterface {
            public function now(): DateTimeImmutable
            {
                return new DateTimeImmutable();
            }
        };
    }

    public function load(VirtualStreamName|StreamName $streamName, EventStreamFilter $filter = null): EventStreamInterface
    {
        $this->reconnectDatabaseConnection();
        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->eventTableName)
            ->orderBy('sequencenumber', 'ASC');

        $queryBuilder = match ($streamName::class) {
            StreamName::class => $queryBuilder->andWhere('stream = :streamName')->setParameter('streamName', $streamName->value),
            VirtualStreamName::class => match ($streamName->type) {
                VirtualStreamType::ALL => $queryBuilder,
                VirtualStreamType::CATEGORY => $queryBuilder->andWhere('stream LIKE :streamNamePrefix')->setParameter('streamNamePrefix', $streamName->value . '%'),
                VirtualStreamType::CORRELATION_ID => $queryBuilder->andWhere('correlationId LIKE :correlationId')->setParameter('correlationId', $streamName->value),
            },
        };
        if ($filter !== null && $filter->eventTypes !== null) {
            $queryBuilder->andWhere('type IN (:eventTypes)')->setParameter('eventTypes', $filter->eventTypes->toStringArray(), ArrayParameterType::STRING);
        }
        return BatchEventStream::create(DoctrineEventStream::create($queryBuilder), 100);
    }

    public function commit(StreamName $streamName, Event|Events $events, ExpectedVersion $expectedVersion): CommitResult
    {
        if ($events instanceof Event) {
            $events = Events::fromArray([$events]);
        }
        # Exponential backoff: initial interval = 5ms and 8 retry attempts = max 1275ms (= 1,275 seconds)
        # @see http://backoffcalculator.com/?attempts=8&rate=2&interval=5
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 8;
        $retryAttempt = 0;
        while (true) {
            $this->reconnectDatabaseConnection();
            if ($this->connection->getTransactionNestingLevel() > 0) {
                throw new \RuntimeException('A transaction is active already, can\'t commit events!', 1547829131);
            }
            $this->connection->beginTransaction();
            try {
                $maybeVersion = $this->getStreamVersion($streamName);
                $expectedVersion->verifyVersion($maybeVersion);
                $version = $maybeVersion->isNothing() ? Version::first() : $maybeVersion->unwrap()->next();
                $lastCommittedVersion = $version;
                foreach ($events as $event) {
                    $this->commitEvent($streamName, $event, $version);
                    $lastCommittedVersion = $version;
                    $version = $version->next();
                }
                $lastInsertId = $this->connection->lastInsertId();
                if (!is_numeric($lastInsertId)) {
                    throw new \RuntimeException(sprintf('Expected last insert id to be numeric, but it is: %s', get_debug_type($lastInsertId)), 1651749706);
                }
                $this->connection->commit();
                return new CommitResult($lastCommittedVersion, SequenceNumber::fromInteger((int)$lastInsertId));
            } catch (UniqueConstraintViolationException $exception) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    $this->connection->rollBack();
                    throw new ConcurrencyException(sprintf('Failed after %d retry attempts', $retryAttempt), 1573817175, $exception);
                }
                usleep((int)($retryWaitInterval * 1E6));
                $retryAttempt++;
                $retryWaitInterval *= 2;
                $this->connection->rollBack();
                continue;
            } catch (DeadlockException | LockWaitTimeoutException $exception) {
                $this->connection->rollBack();
                throw new ConcurrencyException($exception->getMessage(), 1705330559, $exception);
            } catch (DbalException | ConcurrencyException | \JsonException $exception) {
                $this->connection->rollBack();
                throw $exception;
            }
        }
    }

    public function deleteStream(StreamName $streamName): void
    {
        $this->connection->delete($this->eventTableName, [
            'stream' => $streamName->value
        ]);
    }

    public function status(): Status
    {
        try {
            $this->connection->connect();
        } catch (DbalException $e) {
            return Status::error(sprintf('Failed to connect to database: %s', $e->getMessage()));
        }
        $requiredSqlStatements = $this->determineRequiredSqlStatements();
        if ($requiredSqlStatements !== []) {
            return Status::setupRequired(sprintf('The following SQL statement%s required: %s', count($requiredSqlStatements) !== 1 ? 's are' : ' is', implode(chr(10), $requiredSqlStatements)));
        }
        return Status::ok();
    }

    public function setup(): void
    {
        foreach ($this->determineRequiredSqlStatements() as $statement) {
            $this->connection->executeStatement($statement);
        }
    }

    /**
     * @return array<string>
     */
    private function determineRequiredSqlStatements(): array
    {
        $schemaManager = $this->connection->createSchemaManager();
        assert($schemaManager !== null);
        $platform = $this->connection->getDatabasePlatform();
        assert($platform !== null);
        if (!$schemaManager->tablesExist([$this->eventTableName])) {
            return $platform->getCreateTableSQL($this->createEventStoreSchema($schemaManager)->getTable($this->eventTableName));
        }
        $tableSchema = $schemaManager->introspectTable($this->eventTableName);
        $fromSchema = new Schema([$tableSchema], [], $schemaManager->createSchemaConfig());
        $schemaDiff = $schemaManager->createComparator()->compareSchemas($fromSchema, $this->createEventStoreSchema($schemaManager));
        return $platform->getAlterSchemaSQL($schemaDiff);
    }

    // ----------------------------------

    /**
     * Creates the Doctrine schema to be compared with the current db schema for migration
     *
     * @param AbstractSchemaManager<AbstractPlatform> $schemaManager
     * @return Schema
     */
    private function createEventStoreSchema(AbstractSchemaManager $schemaManager): Schema
    {
        $isSQLite = $this->connection->getDatabasePlatform() instanceof SqlitePlatform;
        $table = new Table($this->eventTableName, [
            // The monotonic sequence number
            (new Column('sequencenumber', Type::getType($isSQLite ? Types::INTEGER : Types::BIGINT)))
                ->setUnsigned(true)
                ->setAutoincrement(true),

            // The stream name, usually in the format "<BoundedContext>:<StreamName>"
            (new Column('stream', Type::getType(Types::STRING)))
                ->setLength(StreamName::MAX_LENGTH)
                ->setPlatformOptions($isSQLite ? [] : ['charset' => 'ascii']),

            // Version of the event in the respective stream
            (new Column('version', Type::getType($isSQLite ? Types::INTEGER : Types::BIGINT)))
                ->setUnsigned(true),

            // The event type, often in the format "<BoundedContext>:<EventType>"
            (new Column('type', Type::getType(Types::STRING)))
                ->setLength(EventType::MAX_LENGTH)
                ->setPlatformOptions($isSQLite ? [] : ['charset' => 'ascii']),

            // The event payload, usually stored as JSON
            (new Column('payload', Type::getType(Types::TEXT))),

            // The event metadata stored as JSON
            (new Column('metadata', Type::getType(Types::JSON)))
                ->setNotnull(false),

            // The unique event id, stored as UUID
            (new Column('id', Type::getType(Types::STRING)))
                ->setFixed(true)
                ->setLength(EventId::MAX_LENGTH)
                ->setPlatformOptions($isSQLite ? [] : ['charset' => 'ascii']),

            // An optional causation id, usually a UUID
            (new Column('causationid', Type::getType(Types::STRING)))
                ->setNotnull(false)
                ->setLength(CausationId::MAX_LENGTH)
                ->setPlatformOptions($isSQLite ? [] : ['charset' => 'ascii']),

            // An optional correlation id, usually a UUID
            (new Column('correlationid', Type::getType(Types::STRING)))
                ->setNotnull(false)
                ->setLength(CorrelationId::MAX_LENGTH)
                ->setPlatformOptions($isSQLite ? [] : ['charset' => 'ascii']),

            // Timestamp of the event publishing
            (new Column('recordedat', Type::getType(Types::DATETIME_IMMUTABLE))),
        ]);

        $table->setPrimaryKey(['sequencenumber']);
        $table->addUniqueIndex(['id']);
        $table->addUniqueIndex(['stream', 'version']);
        $table->addIndex(['correlationid']);

        $schemaConfiguration = $schemaManager->createSchemaConfig();
        $schemaConfiguration->setDefaultTableOptions(['charset' => 'utf8mb4']);
        return new Schema([$table], [], $schemaConfiguration);
    }

    /**
     * @throws DriverException
     * @throws DbalException
     */
    private function getStreamVersion(StreamName $streamName): MaybeVersion
    {
        $result = $this->connection->createQueryBuilder()
            ->select('MAX(version)')
            ->from($this->eventTableName)
            ->where('stream = :streamName')
            ->setParameter('streamName', $streamName->value)
            ->executeQuery();
        if (!$result instanceof Result) {
            throw new \RuntimeException(sprintf('Failed to determine stream version of stream "%s"', $streamName->value), 1651153859);
        }
        $version = $result->fetchOne();
        return MaybeVersion::fromVersionOrNull(is_numeric($version) ? Version::fromInteger((int)$version) : null);
    }

    /**
     * @throws DbalException | UniqueConstraintViolationException| \JsonException
     */
    private function commitEvent(StreamName $streamName, Event $event, Version $version): void
    {
        $this->connection->insert(
            $this->eventTableName,
            [
                'id' => $event->id->value,
                'stream' => $streamName->value,
                'version' => $version->value,
                'type' => $event->type->value,
                'payload' => $event->data->value,
                'metadata' => $event->metadata?->toJson(),
                'causationid' => $event->causationId?->value,
                'correlationid' => $event->correlationId?->value,
                'recordedat' => $this->clock->now(),
            ],
            [
                'version' => Types::BIGINT,
                'recordedat' => Types::DATETIME_IMMUTABLE,
            ]
        );
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->fetchOne('SELECT 1');
        } catch (\Exception $_) {
            $this->connection->close();
            $this->connection->connect();
        }
    }
}
