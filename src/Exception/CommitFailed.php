<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Exception;

use Doctrine\DBAL\Exception as DBALException;
use Neos\EventStore\Model\EventsForCommit;

/**
 * @internal
 */
class CommitFailed extends \RuntimeException
{
    public static function becauseConnectionException(EventsForCommit $eventsForCommit, DBALException $connectionException): self
    {
        $firstStreamName = '';
        foreach ($eventsForCommit->eventsForStreams as $eventsForStream) {
            $firstStreamName = $eventsForStream->streamName->value;
        }

        return new self(sprintf('Failed to commit to %d streams (first: %s): %s', $eventsForCommit->eventsForStreams->count(), $firstStreamName, $connectionException->getMessage()), 1787406110, $connectionException);
    }
}
