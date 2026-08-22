<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Exception;

/**
 * @internal
 */
class LockingPlatformFailed extends \RuntimeException
{
    public static function becauseNotImplementedForPlatform(string $name): self
    {
        return new self(sprintf('Locking is not implemented on platform %s', $name), 1787299951);
    }
}
