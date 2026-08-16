<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Model\Event\StreamName;

/**
 * Keeps commits that touch the same stream from running at the same time
 *
 * A commit validates its expected versions with a plain read. For a stream it also writes, a lost race is
 * caught afterwards by the unique index on (stream, version); for a stream it only constrains, nothing is
 * written and there is no index entry to collide with. Holding a lock on every stream a commit touches is
 * what closes that gap – and every stream, not only the constrained ones, because a lock only fences off
 * commits asking for the same lock: a commit that merely writes a stream has to take part as well, or it
 * would remain free to slip in between another commit's constraint check and its COMMIT.
 *
 * The locks are advisory rather than row or gap locks, so that this does not depend on the transaction
 * isolation level: MySQL's SELECT ... FOR UPDATE would fence off inserts under REPEATABLE READ but
 * silently protect nothing under READ COMMITTED.
 */
final class StreamLocks
{
    /**
     * How long a commit waits for the lock of a stream another commit is currently holding
     */
    private const LOCK_TIMEOUT_SECONDS = 10;

    private ?StreamLockMode $mode = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly string $eventTableName,
    ) {
    }

    /**
     * Locks every one of the given streams, or none of them
     *
     * @param list<string> $streamNames in any order – they are locked in a stable one
     * @throws ConcurrencyException if a lock could not be obtained in time
     */
    public function acquire(array $streamNames): AcquiredStreamLocks
    {
        $mode = $this->mode();
        if ($mode === StreamLockMode::Implicit) {
            return new AcquiredStreamLocks($this, []);
        }
        // A total order over the locks is what keeps two commits over overlapping stream sets from
        // deadlocking against each other: both walk the same order, so one of them always gets all its locks
        sort($streamNames);
        $acquiredLocks = [];
        try {
            foreach ($streamNames as $streamName) {
                $lockIdentity = $this->lockIdentity($streamName);
                if ($mode === StreamLockMode::AdvisoryPostgres) {
                    $this->connection->executeStatement('SELECT pg_advisory_lock(CAST(? AS bigint))', [self::postgresLockKey($lockIdentity)]);
                } else {
                    // 1 = acquired, 0 = timed out, NULL = the lock could not be obtained at all
                    $acquired = $this->connection->fetchOne('SELECT GET_LOCK(?, ?)', [$lockIdentity, self::LOCK_TIMEOUT_SECONDS]);
                    if (!is_numeric($acquired) || (int)$acquired !== 1) {
                        throw new ConcurrencyException(sprintf('Failed to acquire lock for stream "%s" within %d seconds', $streamName, self::LOCK_TIMEOUT_SECONDS), 1781012038);
                    }
                }
                $acquiredLocks[] = $lockIdentity;
            }
        } catch (\Throwable $exception) {
            $this->release($acquiredLocks);
            throw $exception;
        }
        return new AcquiredStreamLocks($this, $acquiredLocks);
    }

    /**
     * @see StreamLockMode::resolvesContentionByAborting()
     */
    public function resolvesContentionByAborting(): bool
    {
        return $this->mode()->resolvesContentionByAborting();
    }

    /**
     * @internal to be called through {@see AcquiredStreamLocks::release()}
     * @param list<string> $lockIdentities
     */
    public function release(array $lockIdentities): void
    {
        if ($lockIdentities === []) {
            return;
        }
        $mode = $this->mode();
        // only the locks this connection actually took are released, so that advisory locks the
        // application might use for its own purposes on the same connection are left alone
        foreach ($lockIdentities as $lockIdentity) {
            try {
                if ($mode === StreamLockMode::AdvisoryPostgres) {
                    $this->connection->executeStatement('SELECT pg_advisory_unlock(CAST(? AS bigint))', [self::postgresLockKey($lockIdentity)]);
                } elseif ($mode === StreamLockMode::AdvisoryMySql) {
                    $this->connection->executeStatement('SELECT RELEASE_LOCK(?)', [$lockIdentity]);
                }
            } catch (DbalException $_) {
                // A lock that cannot be released is a lock that is no longer held: advisory locks live and
                // die with the session, so a connection that is gone has dropped them already. Releasing
                // happens in a finally block, where throwing would replace the exception that got us there
                // – or turn an already committed commit into a failed one.
            }
        }
    }

    // ----------------------------------

    /**
     * Resolved lazily and only once: determining the platform can cost a round trip to the database, which
     * has no business happening while the event store is merely being constructed
     */
    private function mode(): StreamLockMode
    {
        return $this->mode ??= StreamLockMode::forPlatform($this->connection->getDatabasePlatform());
    }

    /**
     * A stable lock identity for a stream
     *
     * Stream names can be {@see StreamName::MAX_LENGTH} characters long while MySQL lock names are limited
     * to 64, so the name is hashed rather than used as it is. Nothing here is a security decision – any
     * short, stable digest does, and a collision would only make two unrelated streams share a lock, never
     * lose one. The event table is part of it, so that two event stores in the same database do not lock
     * each other.
     */
    private function lockIdentity(string $streamName): string
    {
        return md5($this->eventTableName . ':' . $streamName);
    }

    /**
     * PostgreSQL advisory locks are keyed by a 64 bit integer rather than by name, so the first 60 bits of
     * the identity are used
     */
    private static function postgresLockKey(string $lockIdentity): int
    {
        return (int)hexdec(substr($lockIdentity, 0, 15));
    }
}
