<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
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

    public function __construct(
        private readonly Connection $connection,
        private readonly string $tableName,
        private readonly string $subscriberId,
    ) {}

    public function acquireLock(): SequenceNumber
    {
        if ($this->connection->isTransactionActive()) {
            throw new CheckpointException(sprintf('Failed to acquire checkpoint lock for subscriber "%s" because a transaction is active already', $this->subscriberId), 1652268416);
        }
        $this->connection->beginTransaction();
        try {
            $highestAppliedSequenceNumber = $this->connection->fetchOne('SELECT appliedsequencenumber FROM ' . $this->connection->quoteIdentifier($this->tableName) . ' WHERE subscriberid = :subscriberId ' . $this->connection->getDatabasePlatform()->getForUpdateSQL() . ' NOWAIT', [
                'subscriberId' => $this->subscriberId
            ]);
        } catch (DriverException $exception) {
            $this->connection->rollBack();
            // Error code "1205" = ER_LOCK_WAIT_TIMEOUT in MySQL (https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html#error_er_lock_wait_timeout)
            // SQL State "55P03" = lock_not_available in PostgreSQL (https://www.postgresql.org/docs/9.4/errcodes-appendix.html)
            if ($exception->getErrorCode() === 1205 || $exception->getSQLState() === '55P03') {
                throw new CheckpointException(sprintf('Failed to acquire checkpoint lock for subscriber "%s" because it is acquired already', $this->subscriberId), 1652279016);
            }
            throw new \RuntimeException($exception->getMessage(), 1544207633, $exception);
        } catch (DBALException $exception) {
            $this->connection->rollBack();
            throw new \RuntimeException($exception->getMessage(), 1544207778, $exception);
        }
        if (!is_numeric($highestAppliedSequenceNumber)) {
            throw new CheckpointException(sprintf('Failed to fetch highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1652279139);
        }
        return SequenceNumber::fromInteger((int)$highestAppliedSequenceNumber);
    }

    public function updateAndReleaseLock(SequenceNumber $sequenceNumber): void
    {
        if (!$this->connection->isTransactionActive()) {
            throw new CheckpointException(sprintf('Failed to update and commit checkpoint for subscriber "%s" because no transaction is active', $this->subscriberId), 1652279314);
        }
        // Fails if no matching entry exists; which is fine because initializeHighestAppliedSequenceNumber() must be called beforehand.
        try {
            $this->connection->update(
                $this->tableName,
                ['appliedsequencenumber' => $sequenceNumber->value],
                ['subscriberid' => $this->subscriberId]
            );
            $this->connection->commit();
        } catch (DBALException $exception) {
            throw new CheckpointException(sprintf('Failed to update and commit highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1652279375, $exception);
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
        $schema = new Schema();
        $table = $schema->createTable($this->tableName);
        $table->addColumn('subscriberid', Types::STRING, ['length' => 255]);
        $table->addColumn('appliedsequencenumber', Types::INTEGER);
        $table->setPrimaryKey(['subscriberid']);

        $schemaDiff = (new Comparator())->compare($schemaManager->createSchema(), $schema);
        foreach ($schemaDiff->toSaveSql($this->connection->getDatabasePlatform()) as $statement) {
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
