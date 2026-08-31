<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Exception;

use Doctrine\DBAL\Exception as DBALException;

/**
 * @internal
 */
class AcquiringLockFailed extends \RuntimeException
{
    public static function becauseMySqlTimeoutExceeded(string $key, string $tableName, int $timeoutInSeconds): self
    {
        return new self(sprintf('Failed to acquire lock for %s ("%s") within %s seconds.', $tableName, $key, $timeoutInSeconds), 1780670522);
    }

    public static function becauseUnexpectedMySqlError(string $key, string $tableName, mixed $result): self
    {
        return new self(sprintf('Failed to acquire lock for %s ("%s"). Unexpected error. Returned %s.', $tableName, $key, json_encode($result)), 1787400700);
    }

    public static function becauseConnectionException(string|int $key, string $tableName, DBALException $connectionException): self
    {
        return new self(sprintf('Failed to acquire lock for %s (%s): %s', $tableName, json_encode($key), $connectionException->getMessage()), 1780671125, $connectionException);
    }
}
