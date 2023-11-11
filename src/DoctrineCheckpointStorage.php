<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException as DBALDriverException;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\MySqlPlatform;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Types;
use Neos\EventStore\CatchUp\CheckpointStorageInterface;
use Neos\EventStore\Exception\CheckpointException;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\EventStore\SetupResult;
use Neos\EventStore\ProvidesSetupInterface;

final class DoctrineCheckpointStorage implements CheckpointStorageInterface, ProvidesSetupInterface
{

    private MySqlPlatform|PostgreSqlPlatform $platform;
    private SequenceNumber|null $lockedSequenceNumber = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly string $tableName,
        private readonly string $subscriberId,
    ) {
        $platform = $this->connection->getDatabasePlatform();
        if (!($platform instanceof MySqlPlatform || $platform instanceof PostgreSqlPlatform)) {
            throw new \InvalidArgumentException(sprintf('The %s only supports the platforms %s and %s currently. Given: %s', $this::class, MySqlPlatform::class, PostgreSqlPlatform::class, get_debug_type($platform)), 1660556004);
        }
        $this->platform = $platform;
    }

    public function acquireLock(): SequenceNumber
    {
        if ($this->connection->isTransactionActive()) {
            throw new CheckpointException(sprintf('Failed to acquire checkpoint lock for subscriber "%s" because a transaction is active already', $this->subscriberId), 1652268416);
        }
        $this->connection->beginTransaction();
        try {
            $highestAppliedSequenceNumber = $this->connection->fetchOne('SELECT appliedsequencenumber FROM ' . $this->connection->quoteIdentifier($this->tableName) . ' WHERE subscriberid = :subscriberId ' . $this->platform->getForUpdateSQL() . ' NOWAIT', [
                'subscriberId' => $this->subscriberId
            ]);
        } catch (DBALException $exception) {
            $this->connection->rollBack();
            if ($exception instanceof LockWaitTimeoutException || ($exception instanceof DBALDriverException && ($exception->getErrorCode() === 3572 || $exception->getErrorCode() === 7))) {
                throw new CheckpointException(sprintf('Failed to acquire checkpoint lock for subscriber "%s" because it is acquired already', $this->subscriberId), 1652279016);
            }
            throw new \RuntimeException($exception->getMessage(), 1544207778, $exception);
        }
        if (!is_numeric($highestAppliedSequenceNumber)) {
            $this->connection->rollBack();
            throw new CheckpointException(sprintf('Failed to fetch highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1652279139);
        }
        $this->lockedSequenceNumber = SequenceNumber::fromInteger((int)$highestAppliedSequenceNumber);
        return $this->lockedSequenceNumber;
    }

    public function updateAndReleaseLock(SequenceNumber $sequenceNumber): void
    {
        if ($this->lockedSequenceNumber === null) {
            throw new CheckpointException(sprintf('Failed to update and commit checkpoint for subscriber "%s" because the lock has not been acquired successfully before', $this->subscriberId), 1660556344);
        }
        if (!$this->connection->isTransactionActive()) {
            throw new CheckpointException(sprintf('Failed to update and commit checkpoint for subscriber "%s" because no transaction is active', $this->subscriberId), 1652279314);
        }
        try {
            if (!$this->lockedSequenceNumber->equals($sequenceNumber)) {
                $this->connection->update($this->tableName, ['appliedsequencenumber' => $sequenceNumber->value], ['subscriberid' => $this->subscriberId]);
            }
            $this->connection->commit();
        } catch (DBALException $exception) {
            $this->connection->rollBack();
            throw new CheckpointException(sprintf('Failed to update and commit highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1652279375, $exception);
        } finally {
            $this->lockedSequenceNumber = null;
        }
    }

    public function getHighestAppliedSequenceNumber(): SequenceNumber
    {
        $highestAppliedSequenceNumber = $this->connection->fetchOne('SELECT appliedsequencenumber FROM ' . $this->connection->quoteIdentifier($this->tableName) . ' WHERE subscriberid = :subscriberId ', [
            'subscriberId' => $this->subscriberId
        ]);
        if (!is_numeric($highestAppliedSequenceNumber)) {
            throw new CheckpointException(sprintf('Failed to fetch highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1652279427);
        }
        return SequenceNumber::fromInteger((int)$highestAppliedSequenceNumber);
    }

    public function setup(): SetupResult
    {
        $schemaManager = $this->connection->getSchemaManager();
        if (!$schemaManager instanceof AbstractSchemaManager) {
            throw new \RuntimeException('Failed to retrieve Schema Manager', 1652269057);
        }
        $schemaConfiguration = $schemaManager->createSchemaConfig();
        $schema = new Schema([], [], $schemaConfiguration);
        $table = $schema->createTable($this->tableName);
        $table->addColumn('subscriberid', Types::STRING, ['length' => 255]);
        $table->addColumn('appliedsequencenumber', Types::INTEGER);
        $table->setPrimaryKey(['subscriberid']);

        $schemaDiff = (new Comparator())->compare($schemaManager->createSchema(), $schema);
        foreach ($schemaDiff->toSaveSql($this->platform) as $statement) {
            $this->connection->executeStatement($statement);
        }
        try {
            $this->connection->insert($this->tableName, ['subscriberid' => $this->subscriberId, 'appliedsequencenumber' => 0]);
        } catch (UniqueConstraintViolationException $e) {
            // table and row already exists, ignore
        }

        return SetupResult::success('');
    }
}
