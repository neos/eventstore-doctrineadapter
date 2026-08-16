<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;

/**
 * How a platform keeps two commits that touch the same stream apart
 *
 * An implementation detail of {@see StreamLocks} – the event store never sees it.
 */
enum StreamLockMode
{
    /**
     * MySQL and MariaDB: named advisory locks, via GET_LOCK() / RELEASE_LOCK()
     */
    case AdvisoryMySql;

    /**
     * PostgreSQL: session level advisory locks, via pg_advisory_lock() / pg_advisory_unlock()
     */
    case AdvisoryPostgres;

    /**
     * SQLite: nothing to acquire, because it allows a single writer at a time anyway – a concurrent commit
     * can neither take the write lock while this transaction is still reading (rollback journal), nor
     * commit ahead of it without this transaction's own INSERT being rejected (WAL, SQLITE_BUSY_SNAPSHOT)
     */
    case Implicit;

    public static function forPlatform(AbstractPlatform $platform): self
    {
        return match (true) {
            $platform instanceof AbstractMySQLPlatform => self::AdvisoryMySql,
            $platform instanceof PostgreSQLPlatform => self::AdvisoryPostgres,
            $platform instanceof SqlitePlatform => self::Implicit,
            default => throw new \RuntimeException(sprintf('Platform %s is not supported: committing events requires a way to lock a stream, and only MySQL/MariaDB, PostgreSQL and SQLite provide one', $platform::class), 1781012037),
        };
    }

    /**
     * Whether contention surfaces as a commit the database aborted rather than as one that had to wait
     *
     * This is what makes a lock error retryable: with nothing to queue on, the database resolves a
     * conflict by aborting the loser – SQLite does so without even consulting its busy timeout once a
     * reading transaction needs to become a writing one – and the documented remedy is to start over.
     * Where commits queue on a lock instead, the same error means the wait for it ran out, and trying
     * again would merely prolong it.
     */
    public function resolvesContentionByAborting(): bool
    {
        return $this === self::Implicit;
    }
}
