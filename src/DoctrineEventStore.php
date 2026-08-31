<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Neos\EventStore\DoctrineAdapter\Exception\AcquiringLockFailed;
use Neos\EventStore\DoctrineAdapter\Exception\CommitFailed;
use Neos\EventStore\DoctrineAdapter\Exception\LockingPlatformFailed;
use Neos\EventStore\DoctrineAdapter\Exception\ReleasingLockFailed;
use Neos\EventStore\DoctrineAdapter\Helper\AdvisoryLockKey;
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
use Neos\EventStore\Model\EventsForCommit;
use Neos\EventStore\Model\EventStore\CommitAllResult;
use Neos\EventStore\Model\EventStore\CommitResult;
use Neos\EventStore\Model\EventStore\Status;
use Neos\EventStore\Model\EventStore\VersionForStream;
use Neos\EventStore\Model\EventStore\VersionForStreams;
use Neos\EventStore\Model\EventStream\EventStreamFilter;
use Neos\EventStore\Model\EventStream\EventStreamInterface;
use Neos\EventStore\Model\EventStream\ExpectedVersion;
use Neos\EventStore\Model\EventStream\MaybeVersion;
use Neos\EventStore\Model\EventStream\VirtualStreamName;
use Neos\EventStore\Model\EventStream\VirtualStreamType;
use Neos\EventStore\WithResetInterface;
use Psr\Clock\ClockInterface;

final class DoctrineEventStore implements EventStoreInterface, WithResetInterface
{
    private const MYSQL_LOCK_TIMEOUT = 3;

    private AdvisoryLockKey $lockKey;

    /**
     * @param string $eventTableName the table for schema setup and storing the events
     * @param string $advisoryLockSeed to avoid that multiple event-store instances lock each other
     *                                 on either multiple databases on the same database server
     *                                 or on multiple tables in a single database, a unique seed should be specified.
     *                                 That is the name of the database + the application name (or just the event table name).
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly string $eventTableName,
        private readonly ClockInterface $clock,
        string $advisoryLockSeed = ''
    ) {
        $this->lockKey = AdvisoryLockKey::fromString($advisoryLockSeed);
    }

    public function load(VirtualStreamName|StreamName $streamName, ?EventStreamFilter $filter = null): EventStreamInterface
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
            $queryBuilder->andWhere('type IN (:eventTypes)')->setParameter('eventTypes', $filter->eventTypes->toStringArray(), Connection::PARAM_STR_ARRAY);
        }
        return BatchEventStream::create(DoctrineEventStream::create($queryBuilder), 100);
    }

    public function commit(StreamName $streamName, Event|Events $events, ExpectedVersion $expectedVersion): CommitResult
    {
        if ($events instanceof Event) {
            $events = Events::fromArray([$events]);
        }
        return CommitResult::fromCommitAll($this->commitAll(EventsForCommit::createEventsForStreamAndExpectedVersion(
            streamName: $streamName,
            events: $events,
            expectedVersion: $expectedVersion,
        )));
    }

    public function commitAll(EventsForCommit $commit): CommitAllResult
    {
        $this->reconnectDatabaseConnection();
        if ($this->connection->getTransactionNestingLevel() > 0) {
            throw CommitFailed::becauseTransactionIsAlreadyActive();
        }
        $this->connection->beginTransaction();
        try {
            $this->lock();
        } catch (AcquiringLockFailed $acquiringLockFailed) {
            $this->connection->rollBack();
            throw new ConcurrencyException($acquiringLockFailed->getMessage(), 1787399398, $acquiringLockFailed);
        }
        try {
            $initialStreamVersions = [];
            // validation
            foreach ($commit->expectedStreamConstraints as $expectedStreamConstraint) {
                $maybeVersion = $this->getStreamVersion($expectedStreamConstraint->streamName);
                $initialStreamVersions[$expectedStreamConstraint->streamName->value] = $maybeVersion->nextVersionOrFirst();
                if (!$expectedStreamConstraint->isSatisfiedBy($maybeVersion)) {
                    throw ConcurrencyException::becauseVersionOfStreamDoesNotMatchExpectedConstraint($expectedStreamConstraint, $maybeVersion, $commit->expectedStreamConstraints);
                }
            }

            $highestCommittedSequenceNumber = null;
            $newStreamVersions = [];
            foreach ($commit->eventsForStreams as $eventsForStream) {
                $version = ($newStreamVersions[$eventsForStream->streamName->value] ?? null)?->version->next() ?? $initialStreamVersions[$eventsForStream->streamName->value] ?? $this->getStreamVersion($eventsForStream->streamName)->nextVersionOrFirst();
                $lastCommittedVersion = $version;
                foreach ($eventsForStream->events as $event) {
                    $this->commitEvent($eventsForStream->streamName, $event, $version);
                    $lastCommittedVersion = $version;
                    $version = $version->next();
                }
                $lastInsertId = $this->connection->lastInsertId();
                if (!is_numeric($lastInsertId)) {
                    throw CommitFailed::becauseLastInsertIdMustBeNumeric($lastInsertId);
                }
                $highestCommittedSequenceNumber = SequenceNumber::fromInteger((int)$lastInsertId);
                $newStreamVersions[$eventsForStream->streamName->value] = VersionForStream::create($eventsForStream->streamName, $lastCommittedVersion);
            }
            $this->connection->commit();
            // Always set, as at least one iteration
            assert($highestCommittedSequenceNumber !== null);
            return CommitAllResult::create($highestCommittedSequenceNumber, VersionForStreams::create(...array_values($newStreamVersions)));
        } catch (LockWaitTimeoutException $lockWaitTimeoutException) {
            // Thrown in concurrency in SQLite: General error: 5 database is locked
            $this->connection->rollBack();
            throw new ConcurrencyException($lockWaitTimeoutException->getMessage(), 1705330559, $lockWaitTimeoutException);
        } catch (DbalException $exception) {
            $this->connection->rollBack();
            throw CommitFailed::becauseConnectionException($commit, $exception);
        } catch (\Exception $exception) {
            $this->connection->rollBack();
            throw $exception;
        } finally {
            $this->unlock();
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

    public function reset(): void
    {
        if ($this->connection->getDatabasePlatform() instanceof SqlitePlatform) {
            $this->connection->executeStatement('DELETE FROM ' . $this->eventTableName);
            $this->connection->executeStatement('UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME="' . $this->eventTableName . '"');
        } elseif ($this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $this->connection->executeStatement('TRUNCATE TABLE ' . $this->eventTableName . ' RESTART IDENTITY');
        } else {
            $this->connection->executeStatement('TRUNCATE TABLE ' . $this->eventTableName);
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
        $schemaDiff = (new Comparator())->compareSchemas($fromSchema, $this->createEventStoreSchema($schemaManager));
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
            throw CommitFailed::becauseFailedToDetermineStreamVersion($streamName);
        }
        $version = $result->fetchOne();
        return MaybeVersion::fromVersionOrNull(is_numeric($version) ? Version::fromInteger((int)$version) : null);
    }

    /**
     * @throws DbalException | UniqueConstraintViolationException
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
                'recordedat' => $this->clock->now()->setTimezone(new \DateTimeZone('UTC')),
            ],
            [
                'version' => Types::BIGINT,
                'recordedat' => Types::DATETIME_IMMUTABLE,
            ]
        );
    }

    private function lock(): void
    {
        $platform = $this->connection->getDatabasePlatform();

        if ($platform instanceof PostgreSQLPlatform) {
            try {
                $this->connection->executeStatement(
                    'SELECT pg_advisory_xact_lock(?)',
                    [$this->lockKey->as64BitInt()],
                );
            } catch (DbalException $exception) {
                throw AcquiringLockFailed::becauseConnectionException($this->lockKey->as64BitInt(), $this->eventTableName, $exception);
            }

            return;
        }

        if ($platform instanceof MariaDBPlatform || $platform instanceof MySQLPlatform) {
            try {
                $result = $this->connection->fetchOne(
                    'SELECT GET_LOCK(?, ?)',
                    [$this->lockKey->as16CharHexString(), self::MYSQL_LOCK_TIMEOUT],
                );
            } catch (DbalException $exception) {
                throw AcquiringLockFailed::becauseConnectionException($this->lockKey->as16CharHexString(), $this->eventTableName, $exception);
            }

            match ($result) {
                // https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html#function_get-lock
                1 => null,
                0 => throw AcquiringLockFailed::becauseMySqlTimeoutExceeded($this->lockKey->as16CharHexString(), $this->eventTableName, self::MYSQL_LOCK_TIMEOUT),
                default => throw AcquiringLockFailed::becauseUnexpectedMySqlError($this->lockKey->as16CharHexString(), $this->eventTableName, $result)
            };

            return;
        }

        if ($platform instanceof SQLitePlatform) {
            return; // sql locking is not needed because of file locking
        }

        throw new LockingPlatformFailed($platform::class);
    }

    private function unlock(): void
    {
        $platform = $this->connection->getDatabasePlatform();

        if ($platform instanceof PostgreSQLPlatform) {
            return; // lock is released automatically after transaction
        }

        if ($platform instanceof MariaDBPlatform || $platform instanceof MySQLPlatform) {
            try {
                $result = $this->connection->fetchOne(
                    'SELECT RELEASE_LOCK(?)',
                    [$this->lockKey->as16CharHexString()],
                );
            } catch (DbalException $exception) {
                throw ReleasingLockFailed::becauseConnectionException($this->lockKey->as16CharHexString(), $this->eventTableName, $exception);
            }

            match ($result) {
                // https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html#function_release-lock
                1 => null,
                0 => throw ReleasingLockFailed::becauseMySqlLockWasNotHereAcquired($this->lockKey->as16CharHexString(), $this->eventTableName),
                null => throw ReleasingLockFailed::becauseMySqlLockDoesNotExist($this->lockKey->as16CharHexString(), $this->eventTableName),
                default => throw ReleasingLockFailed::becauseUnexpectedMySqlError($this->lockKey->as16CharHexString(), $this->eventTableName, $result)
            };

            return;
        }

        if ($platform instanceof SQLitePlatform) {
            return; // sql locking is not needed because of file locking
        }

        throw LockingPlatformFailed::becauseNotImplementedForPlatform($platform::class);
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
