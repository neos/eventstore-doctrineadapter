<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

/**
 * The stream locks a commit is currently holding, and the only way to give them back
 *
 * Handing out a handle rather than the bare list of locks is what keeps the pairing honest: there is no
 * second set of names a caller could release by mistake, and no set it could forget to.
 *
 * @internal returned by {@see StreamLocks::acquire()}
 */
final class AcquiredStreamLocks
{
    /**
     * @param list<string> $lockIdentities
     */
    public function __construct(
        private readonly StreamLocks $streamLocks,
        private array $lockIdentities,
    ) {
    }

    /**
     * Releases the locks. Calling this more than once is safe and does nothing the second time, so it
     * belongs in a finally block without further ceremony.
     */
    public function release(): void
    {
        $lockIdentities = $this->lockIdentities;
        $this->lockIdentities = [];
        $this->streamLocks->release($lockIdentities);
    }
}
