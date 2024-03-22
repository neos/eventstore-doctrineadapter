<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform; // @phpstan-ignore-line
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Model\EventStore\StatusType;
use Neos\EventStore\Tests\Integration\AbstractEventStoreTestBase;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(DoctrineEventStore::class)]
final class DoctrineEventStoreTest extends AbstractEventStoreTestBase
{
    private static ?Connection $connection = null;

    protected static function createEventStore(): EventStoreInterface
    {
        return new DoctrineEventStore(self::connection(), self::eventTableName());
    }

    protected static function resetEventStore(): void
    {
        $connection = self::connection();
        if (!$connection->getSchemaManager()->tablesExist([self::eventTableName()])) {
            return;
        }
        if ($connection->getDatabasePlatform() instanceof SqlitePlatform) {
            $connection->executeStatement('DELETE FROM ' . self::eventTableName());
            $connection->executeStatement('UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME="' . self::eventTableName() . '"');
        } elseif ($connection->getDatabasePlatform() instanceof PostgreSQLPlatform) { // @phpstan-ignore-line
            $connection->executeStatement('TRUNCATE TABLE ' . self::eventTableName() . ' RESTART IDENTITY');
        } else {
            $connection->executeStatement('TRUNCATE TABLE ' . self::eventTableName());
        }
    }

    public static function connection(): Connection
    {
        if (self::$connection === null) {
            $dsn = getenv('DB_DSN');
            if (!is_string($dsn)) {
                $dsn = 'sqlite:///events_test.sqlite';
            }
            self::$connection = DriverManager::getConnection(['url' => $dsn]);
        }
        return self::$connection;
    }

    public static function eventTableName(): string
    {
        return 'events_test';
    }

    public function test_setup_throws_exception_if_database_connection_fails(): void
    {
        $connection = DriverManager::getConnection(['url' => 'mysql://invalid-connection']);
        $eventStore = new DoctrineEventStore($connection, self::eventTableName());

        $this->expectException(DbalException::class);
        $eventStore->setup();
    }

    public function test_status_returns_error_status_if_database_connection_fails(): void
    {
        $connection = DriverManager::getConnection(['url' => 'mysql://invalid-connection']);
        $eventStore = new DoctrineEventStore($connection, self::eventTableName());
        self::assertSame($eventStore->status()->type, StatusType::ERROR);
    }

    public function test_status_returns_setup_required_status_if_event_table_is_missing(): void
    {
        $connection = DriverManager::getConnection(['url' => 'sqlite:///:memory:']);
        $eventStore = new DoctrineEventStore($connection, self::eventTableName());
        self::assertSame($eventStore->status()->type, StatusType::SETUP_REQUIRED);
    }

    public function test_status_returns_setup_required_status_if_event_table_requires_update(): void
    {
        $connection = self::connection();
        $eventStore = new DoctrineEventStore($connection, self::eventTableName());
        $eventStore->setup();
        $connection->executeStatement('ALTER TABLE ' . self::eventTableName() . ' RENAME COLUMN metadata TO metadata_renamed');
        self::assertSame($eventStore->status()->type, StatusType::SETUP_REQUIRED);
    }

    public function test_status_returns_ok_status_if_event_table_is_up_to_date(): void
    {
        $connection = self::connection();
        $eventStore = new DoctrineEventStore($connection, self::eventTableName());
        $eventStore->setup();
        self::assertSame($eventStore->status()->type, StatusType::OK);
    }
}
