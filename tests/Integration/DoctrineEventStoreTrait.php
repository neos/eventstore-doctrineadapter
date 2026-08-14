<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\EventStoreInterface;

trait DoctrineEventStoreTrait
{
    /**
     * How long a SQLite connection waits for the write lock before giving up
     */
    private const SQLITE_BUSY_TIMEOUT_SECONDS = 30;

    private static ?Connection $connection = null;

    protected static function createEventStore(): EventStoreInterface
    {
        return new DoctrineEventStore(self::connection(), self::eventTableName(), \Neos\EventStore\Tests\Integration\EventStoreFakeClock::get());
    }

    protected static function eventStoreDescription(): string
    {
        $connection = self::connection();
        $parameters = $connection->getParams();
        // file based platforms (SQLite) have a path rather than a meaningful database name
        $database = $parameters['path'] ?? $connection->getDatabase() ?? 'unknown';
        return sprintf(
            'Doctrine DBAL, platform %s, driver %s, database "%s", table "%s"',
            (new \ReflectionClass($connection->getDatabasePlatform()))->getShortName(),
            $parameters['driver'] ?? 'unknown',
            $database,
            self::eventTableName(),
        );
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
        } elseif ($connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
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
            $params = ['url' => $dsn];
            if (str_starts_with($dsn, 'sqlite')) {
                // SQLite allows a single writer at a time. Without a busy timeout the loser of a race
                // fails immediately with "database is locked" instead of waiting for its turn, which the
                // consistency tests rightly report as a commit that should have succeeded.
                // Passed as a driver option rather than as a PRAGMA so that it survives the reconnect in
                // DoctrineEventStore::reconnectDatabaseConnection().
                $params['driverOptions'] = [\PDO::ATTR_TIMEOUT => self::SQLITE_BUSY_TIMEOUT_SECONDS];
            }
            self::$connection = DriverManager::getConnection($params);
        }
        return self::$connection;
    }

    public static function eventTableName(): string
    {
        return 'events_test';
    }
}