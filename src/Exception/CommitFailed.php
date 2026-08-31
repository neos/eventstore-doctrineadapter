<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Exception;

use Doctrine\DBAL\Exception as DBALException;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\EventsForCommit;

/**
 * @internal
 */
class CommitFailed extends \RuntimeException
{
    public static function becauseFailedToDetermineStreamVersion(StreamName $streamName): self
    {
        return new self(sprintf('Failed to determine stream version of stream "%s"', $streamName->value), 1651153859);
    }

    public static function becauseTransactionIsAlreadyActive(): self
    {
        return new self('A transaction is active already, can\'t commit events!', 1547829131);
    }

    public static function becauseLastInsertIdMustBeNumeric(mixed $lastInsertId): self
    {
        return new self(sprintf('Expected last insert id to be numeric, but it is: %s', json_encode($lastInsertId)), 1651749706);
    }

    public static function becauseConnectionException(EventsForCommit $eventsForCommit, DBALException $connectionException): self
    {
        $firstStreamName = '';
        foreach ($eventsForCommit->eventsForStreams as $eventsForStream) {
            $firstStreamName = $eventsForStream->streamName->value;
        }

        return new self(sprintf('Failed to commit to %d streams (first: %s): %s', $eventsForCommit->eventsForStreams->count(), $firstStreamName, $connectionException->getMessage()), 1787406110, $connectionException);
    }
}
