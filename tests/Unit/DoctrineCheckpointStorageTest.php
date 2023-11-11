<?php
declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Tests\Unit;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Platforms\MySqlPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Neos\EventStore\DoctrineAdapter\DoctrineCheckpointStorage;
use Neos\EventStore\Exception\CheckpointException;
use Neos\EventStore\Model\Event\SequenceNumber;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

#[CoversClass(DoctrineCheckpointStorage::class)]
final class DoctrineCheckpointStorageTest extends TestCase
{
    private Connection|MockObject $mockConnection;
    private DoctrineCheckpointStorage $checkpointStorage;

    public function setUp(): void
    {
        $this->mockConnection = $this->getMockBuilder(Connection::class)->disableOriginalConstructor()->getMock();
        $mockDatabasePlatform = $this->getMockBuilder(MySqlPlatform::class)->disableOriginalConstructor()->getMock();
        $this->mockConnection->method('getDatabasePlatform')->willReturn($mockDatabasePlatform);

        $this->checkpointStorage = new DoctrineCheckpointStorage($this->mockConnection, 'some_table', 'some_subscriber');
    }

    private function simulateLockAcquired(): void
    {
        $bound = \Closure::bind(static fn &(DoctrineCheckpointStorage $checkpointStorage) => $checkpointStorage->lockedSequenceNumber, null, $this->checkpointStorage);
        /** @noinspection PhpPassByRefInspection */
        $ref = &$bound($this->checkpointStorage);
        $ref = SequenceNumber::none();
        $this->mockConnection->method('isTransactionActive')->willReturn(true);
    }

    public function test_constructor_fails_if_database_platform_cannot_be_determined(): void
    {
        $mockConnection = $this->getMockBuilder(Connection::class)->disableOriginalConstructor()->getMock();
        $mockConnection->expects(self::atLeastOnce())->method('getDatabasePlatform')->willReturn(null);

        $this->expectException(\InvalidArgumentException::class);
        new DoctrineCheckpointStorage($mockConnection, 'some_table', 'some_subscriber');
    }

    public function test_constructor_fails_if_database_platform_is_not_supported(): void
    {
        $mockConnection = $this->getMockBuilder(Connection::class)->disableOriginalConstructor()->getMock();
        $mockDatabasePlatform = $this->getMockBuilder(SqlitePlatform::class)->disableOriginalConstructor()->getMock();
        $mockConnection->expects(self::atLeastOnce())->method('getDatabasePlatform')->willReturn($mockDatabasePlatform);

        $this->expectException(\InvalidArgumentException::class);
        new DoctrineCheckpointStorage($mockConnection, 'some_table', 'some_subscriber');
    }

    public function test_acquireLock_startsTransaction(): void
    {
        $this->mockConnection->expects(self::once())->method('beginTransaction');
        $this->mockConnection->expects(self::once())->method('fetchOne')->willReturn('22');
        $this->checkpointStorage->acquireLock();
    }

    public function test_updateAndReleaseLock_updates_appliedsequencenumber_and_commits_transaction(): void
    {
        $this->simulateLockAcquired();

        $this->mockConnection->expects(self::once())->method('update')->with('some_table', ['appliedsequencenumber' => 123], ['subscriberid' => 'some_subscriber']);
        $this->mockConnection->expects(self::once())->method('commit');
        $this->checkpointStorage->updateAndReleaseLock(SequenceNumber::fromInteger(123));
    }

    public function test_updateAndReleaseLock_rolls_back_transaction_on_exception(): void
    {
        $this->simulateLockAcquired();

        $mockException = $this->getMockBuilder(DBALException::class)->disableOriginalConstructor()->getMock();
        $this->mockConnection->expects(self::once())->method('update')->willThrowException($mockException);

        $this->expectException(CheckpointException::class);
        $this->mockConnection->expects(self::once())->method('rollBack');
        $this->checkpointStorage->updateAndReleaseLock(SequenceNumber::fromInteger(123));
    }

    public function test_updateAndReleaseLock_does_not_update_sequenceNumber_if_it_has_not_been_changed(): void
    {
        $this->mockConnection->expects(self::once())->method('fetchOne')->willReturn('22');
        $this->checkpointStorage->acquireLock();

        $this->mockConnection->expects(self::once())->method('isTransactionActive')->willReturn(true);
        $this->mockConnection->expects(self::never())->method('update');
        $this->mockConnection->expects(self::once())->method('commit');
        $this->checkpointStorage->updateAndReleaseLock(SequenceNumber::fromInteger(22));
    }
}
