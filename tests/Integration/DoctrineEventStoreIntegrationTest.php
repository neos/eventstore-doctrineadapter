<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DbalException;
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Model\EventStore\StatusType;
use Neos\EventStore\Tests\Integration\AbstractEventStoreTestBase;
use Neos\EventStore\Tests\Integration\EventStoreFakeClock;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(DoctrineEventStore::class)]
final class DoctrineEventStoreIntegrationTest extends AbstractEventStoreTestBase
{

    use DoctrineEventStoreTrait;

    public function test_setup_throws_exception_if_database_connection_fails(): void
    {
        $connection = DriverManager::getConnection(['url' => 'mysql://invalid-connection']);
        $eventStore = new DoctrineEventStore($connection, self::eventTableName(), EventStoreFakeClock::get());

        $this->expectException(DbalException::class);
        $eventStore->setup();
    }

    public function test_status_returns_error_status_if_database_connection_fails(): void
    {
        $connection = DriverManager::getConnection(['url' => 'mysql://invalid-connection']);
        $eventStore = new DoctrineEventStore($connection, self::eventTableName(), EventStoreFakeClock::get());
        self::assertSame($eventStore->status()->type, StatusType::ERROR);
    }

    public function test_status_returns_setup_required_status_if_event_table_is_missing(): void
    {
        $connection = DriverManager::getConnection(['url' => 'sqlite:///:memory:']);
        $eventStore = new DoctrineEventStore($connection, self::eventTableName(), EventStoreFakeClock::get());
        self::assertSame($eventStore->status()->type, StatusType::SETUP_REQUIRED);
    }

    public function test_status_returns_setup_required_status_if_event_table_requires_update(): void
    {
        $connection = self::connection();
        $eventStore = new DoctrineEventStore($connection, self::eventTableName(), EventStoreFakeClock::get());
        $eventStore->setup();
        $connection->executeStatement('ALTER TABLE ' . self::eventTableName() . ' RENAME COLUMN metadata TO metadata_renamed');
        self::assertSame($eventStore->status()->type, StatusType::SETUP_REQUIRED);
    }

    public function test_status_returns_ok_status_if_event_table_is_up_to_date(): void
    {
        $connection = self::connection();
        $eventStore = new DoctrineEventStore($connection, self::eventTableName(), EventStoreFakeClock::get());
        $eventStore->setup();
        self::assertSame($eventStore->status()->type, StatusType::OK);
    }
}
