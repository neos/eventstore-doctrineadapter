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
            self::$connection = DriverManager::getConnection(['url' => $dsn]);
        }
        return self::$connection;
    }

    public static function eventTableName(): string
    {
        return 'events_test';
    }
}