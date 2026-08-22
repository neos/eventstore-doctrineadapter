<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Exception;

use Doctrine\DBAL\Exception as DBALException;

/**
 * @internal
 */
class ReleasingLockFailed extends \RuntimeException
{
    public static function becauseMySqlLockDoesNotExist(string $key, string $tableName): self
    {
        return new self(sprintf('Failed to release lock for %s ("%s") because it does not exist.', $tableName, $key), 1787401104);
    }

    public static function becauseMySqlLockWasNotHereAcquired(string $key, string $tableName): self
    {
        return new self(sprintf('Failed to release foreign lock for %s ("%s") because it was not established by this session.', $tableName, $key), 1787401151);
    }

    public static function becauseUnexpectedMySqlError(string $key, string $tableName, mixed $result): self
    {
        return new self(sprintf('Failed to release lock for %s ("%s"). Unexpected error. Returned %s.', $tableName, $key, json_encode($result)), 1787401585);
    }

    public static function becauseConnectionException(string|int $key, string $tableName, DBALException $connectionException): self
    {
        return new self(sprintf('Failed to release lock for %s (%s): %s', $tableName, json_encode($key), $connectionException->getMessage()), 1780671125, $connectionException);
    }
}
