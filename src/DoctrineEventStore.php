<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\AbstractPlatform;
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
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Helper\BatchEventStream;
use Neos\EventStore\Model\EventsForCommit;
use Neos\EventStore\Model\Event;
use Neos\EventStore\Model\Event\CausationId;
use Neos\EventStore\Model\Event\CorrelationId;
use Neos\EventStore\Model\Event\EventId;
use Neos\EventStore\Model\Event\EventType;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\Event\Version;
use Neos\EventStore\Model\Events;
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
    /**
     * How long a commit waits for the lock of a stream another commit is currently holding
     */
    private const LOCK_TIMEOUT_SECONDS = 10;

    /**
     * Streams are locked through MySQL's GET_LOCK() / RELEASE_LOCK()
     */
    private const STREAM_LOCKS_ADVISORY_MYSQL = 'advisory-mysql';

    /**
     * Streams are locked through PostgreSQL's pg_advisory_lock() / pg_advisory_unlock()
     */
    private const STREAM_LOCKS_ADVISORY_POSTGRES = 'advisory-postgres';

    /**
     * The platform keeps conflicting commits apart on its own, no explicit lock required
     */
    private const STREAM_LOCKS_IMPLICIT = 'implicit';

    /**
     * There is no way to lock a stream on this platform
     */
    private const STREAM_LOCKS_NONE = 'none';

    public function __construct(
        private readonly Connection $connection,
        private readonly string $eventTableName,
        private readonly ClockInterface $clock
    ) {
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
        # Exponential backoff: initial interval = 5ms and 8 retry attempts = max 1275ms (= 1,275 seconds)
        # @see http://backoffcalculator.com/?attempts=8&rate=2&interval=5
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 8;
        $retryAttempt = 0;

        $this->validateConstraintsOnUnwrittenStreamsAreSupported($commit);
        $streamNamesToLock = self::streamNamesToLock($commit);

        while (true) {
            $this->reconnectDatabaseConnection();
            if ($this->connection->getTransactionNestingLevel() > 0) {
                throw new \RuntimeException('A transaction is active already, can\'t commit events!', 1547829131);
            }
            // Deliberately *outside* the transaction: a lock taken within it would only fence off concurrent
            // writers from the point it is granted onwards, while the version reads could still be answered
            // from a snapshot that predates the commit of whoever held the lock before us
            $acquiredLocks = $this->acquireStreamLocks($streamNamesToLock);
            try {
                $this->connection->beginTransaction();
            } catch (\Throwable $exception) {
                // the locks outlive the transaction that failed to start, so they have to go explicitly –
                // they are bound to the session, not to the transaction
                $this->releaseStreamLocks($acquiredLocks);
                throw $exception;
            }
            $retryDelayMicroseconds = 0;
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
                        throw new \RuntimeException(sprintf('Expected last insert id to be numeric, but it is: %s', get_debug_type($lastInsertId)), 1651749706);
                    }
                    $highestCommittedSequenceNumber = SequenceNumber::fromInteger((int)$lastInsertId);
                    $newStreamVersions[$eventsForStream->streamName->value] = VersionForStream::create($eventsForStream->streamName, $lastCommittedVersion);
                }
                $this->connection->commit();
                // Always set, as at least one iteration
                assert($highestCommittedSequenceNumber !== null);
                return CommitAllResult::create($highestCommittedSequenceNumber, VersionForStreams::create(...array_values($newStreamVersions)));
            } catch (UniqueConstraintViolationException $exception) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    $this->connection->rollBack();
                    throw new ConcurrencyException(sprintf('Failed after %d retry attempts', $retryAttempt), 1573817175, $exception);
                }
                $retryDelayMicroseconds = (int)($retryWaitInterval * 1E6);
                $retryAttempt++;
                $retryWaitInterval *= 2;
                $this->connection->rollBack();
            } catch (DeadlockException | LockWaitTimeoutException $exception) {
                $this->connection->rollBack();
                throw new ConcurrencyException($exception->getMessage(), 1705330559, $exception);
            } catch (DbalException | ConcurrencyException | \JsonException $exception) {
                $this->connection->rollBack();
                throw $exception;
            } finally {
                // after the COMMIT on the success path, so that no concurrent commit can observe the store
                // between this commit's constraint check and the events it wrote
                $this->releaseStreamLocks($acquiredLocks);
            }
            // Only the retrying branch above gets here – every other one returns or throws. The backoff
            // deliberately waits *after* the locks are gone: sleeping while holding them would block the
            // very commits this one is backing off for
            usleep($retryDelayMicroseconds);
        }
    }

    /**
     * The streams a commit has to hold a lock on while it runs, in a stable order
     *
     * All streams it touches, not only the constrained ones. An advisory lock only fences off commits
     * that ask for the same lock, so a commit that merely *writes* a stream has to take part as well –
     * otherwise it would be free to slip in between a concurrent commit's constraint check on that stream
     * and its COMMIT. For a stream that is written *and* constrained the unique index on (stream, version)
     * would catch that on its own, but for a constraint on a stream this commit does not write there is no
     * index entry to collide with, and the lock is the only thing standing between the two.
     *
     * Sorting is what keeps two commits over overlapping stream sets from deadlocking against each other:
     * both walk the same total order, so one of them always gets all its locks.
     *
     * @return list<string>
     */
    private static function streamNamesToLock(EventsForCommit $commit): array
    {
        $streamNames = [];
        foreach ($commit->eventsForStreams as $eventsForStream) {
            $streamNames[$eventsForStream->streamName->value] = true;
        }
        foreach ($commit->expectedStreamConstraints as $expectedStreamConstraint) {
            $streamNames[$expectedStreamConstraint->streamName->value] = true;
        }
        $streamNames = array_keys($streamNames);
        sort($streamNames);
        return $streamNames;
    }

    /**
     * How this platform keeps two commits that touch the same stream apart
     *
     * A single source of truth on purpose: which lock {@see acquireStreamLocks()} takes and which commits
     * {@see validateConstraintsOnUnwrittenStreamsAreSupported()} lets through are two answers to the same
     * question. A platform added to one of them but forgotten in the other would silently accept
     * constraints that nothing guards, which is the one failure mode that leaves no trace.
     *
     * @return self::STREAM_LOCKS_*
     */
    private function streamLocking(): string
    {
        $platform = $this->connection->getDatabasePlatform();
        return match (true) {
            $platform instanceof AbstractMySQLPlatform => self::STREAM_LOCKS_ADVISORY_MYSQL,
            $platform instanceof PostgreSQLPlatform => self::STREAM_LOCKS_ADVISORY_POSTGRES,
            // SQLite allows a single writer at a time: a concurrent commit can neither take the write lock
            // while this transaction is still reading (rollback journal), nor commit ahead of it without
            // this transaction's own INSERT being rejected afterwards (WAL, SQLITE_BUSY_SNAPSHOT)
            $platform instanceof SqlitePlatform => self::STREAM_LOCKS_IMPLICIT,
            default => self::STREAM_LOCKS_NONE,
        };
    }

    /**
     * @param list<string> $streamNames
     * @return list<string> the streams that were actually locked, to be handed to {@see releaseStreamLocks()}
     */
    private function acquireStreamLocks(array $streamNames): array
    {
        $streamLocking = $this->streamLocking();
        if ($streamLocking === self::STREAM_LOCKS_IMPLICIT || $streamLocking === self::STREAM_LOCKS_NONE) {
            return [];
        }
        $acquiredLocks = [];
        try {
            foreach ($streamNames as $streamName) {
                if ($streamLocking === self::STREAM_LOCKS_ADVISORY_POSTGRES) {
                    $this->connection->executeStatement('SELECT pg_advisory_lock(CAST(? AS bigint))', [self::postgresLockKey($this->lockIdentity($streamName))]);
                } else {
                    // 1 = acquired, 0 = timed out, NULL = the lock could not be obtained at all
                    $acquired = $this->connection->fetchOne('SELECT GET_LOCK(?, ?)', [$this->lockIdentity($streamName), self::LOCK_TIMEOUT_SECONDS]);
                    if (!is_numeric($acquired) || (int)$acquired !== 1) {
                        throw new ConcurrencyException(sprintf('Failed to acquire lock for stream "%s" within %d seconds', $streamName, self::LOCK_TIMEOUT_SECONDS), 1781012038);
                    }
                }
                $acquiredLocks[] = $streamName;
            }
        } catch (\Throwable $exception) {
            $this->releaseStreamLocks($acquiredLocks);
            throw $exception;
        }
        return $acquiredLocks;
    }

    /**
     * @param list<string> $streamNames
     */
    private function releaseStreamLocks(array $streamNames): void
    {
        if ($streamNames === []) {
            return;
        }
        $streamLocking = $this->streamLocking();
        // only the locks this connection actually took are released, so that advisory locks the
        // application might use for its own purposes on the same connection are left alone
        foreach ($streamNames as $streamName) {
            try {
                if ($streamLocking === self::STREAM_LOCKS_ADVISORY_POSTGRES) {
                    $this->connection->executeStatement('SELECT pg_advisory_unlock(CAST(? AS bigint))', [self::postgresLockKey($this->lockIdentity($streamName))]);
                } elseif ($streamLocking === self::STREAM_LOCKS_ADVISORY_MYSQL) {
                    $this->connection->executeStatement('SELECT RELEASE_LOCK(?)', [$this->lockIdentity($streamName)]);
                }
            } catch (DbalException $_) {
                // A lock that cannot be released is a lock that is no longer held: advisory locks live and
                // die with the session, so a connection that is gone has dropped them already. Releasing
                // happens in a finally block, where throwing would replace the exception that got us here
                // – or turn an already committed commit into a failed one.
            }
        }
    }

    /**
     * A stable lock identity for a stream, as a 64 character hash
     *
     * Stream names can be {@see StreamName::MAX_LENGTH} characters long while MySQL lock names are limited
     * to 64, so the name is hashed rather than used directly. The event table is part of the hash, so that
     * two event stores sharing one database do not lock each other.
     */
    private function lockIdentity(string $streamName): string
    {
        return hash('sha256', $this->eventTableName . ':' . $streamName);
    }

    /**
     * PostgreSQL advisory locks are keyed by a 64 bit integer rather than by name, so the first 60 bits of
     * the hash are used – a collision costs two unrelated streams a shared lock, never correctness
     */
    private static function postgresLockKey(string $lockIdentity): int
    {
        return (int)hexdec(substr($lockIdentity, 0, 15));
    }

    /**
     * Constraints on streams the commit does not write to are only as good as the lock that guards them
     *
     * On a platform without one, such a commit could still be validated against a version another process
     * has already moved on from, so it is rejected before anything is written rather than silently
     * accepted. Everything else keeps working there: a constraint on a stream the commit *does* write is
     * guarded by the unique index on (stream, version), whether or not the platform can lock.
     */
    private function validateConstraintsOnUnwrittenStreamsAreSupported(EventsForCommit $commit): void
    {
        if ($this->streamLocking() !== self::STREAM_LOCKS_NONE) {
            return;
        }
        $writtenStreamNames = [];
        foreach ($commit->eventsForStreams as $eventsForStream) {
            $writtenStreamNames[$eventsForStream->streamName->value] = true;
        }
        $unwrittenStreamNames = [];
        foreach ($commit->expectedStreamConstraints as $expectedStreamConstraint) {
            if (!isset($writtenStreamNames[$expectedStreamConstraint->streamName->value])) {
                $unwrittenStreamNames[$expectedStreamConstraint->streamName->value] = true;
            }
        }
        if ($unwrittenStreamNames !== []) {
            throw new \RuntimeException(sprintf('Constraints on streams that are not written to ([%s]) are not supported on platform %s because it provides no way to lock a stream', join(', ', array_keys($unwrittenStreamNames)), $this->connection->getDatabasePlatform()::class), 1781012037);
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
                'recordedat' => $this->clock->now()->setTimezone(new \DateTimeZone('UTC')),
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
