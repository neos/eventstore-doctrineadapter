<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\ForwardCompatibility\Result;
use Doctrine\DBAL\Query\QueryBuilder;
use Neos\EventStore\Model\Event;
use Neos\EventStore\Model\Event\EventData;
use Neos\EventStore\Model\Event\EventId;
use Neos\EventStore\Model\Event\EventMetadata;
use Neos\EventStore\Model\EventStream\EventStreamInterface;
use Neos\EventStore\Model\Event\EventType;
use Neos\EventStore\Model\EventEnvelope;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\Event\Version;

final class DoctrineEventStream implements EventStreamInterface
{

    private function __construct(
        private QueryBuilder $queryBuilder,
        private readonly ?SequenceNumber $minimumSequenceNumber,
        private readonly ?SequenceNumber $maximumSequenceNumber,
        private readonly ?int $limit,
        private readonly bool $backwards,
    ) {
    }

    public static function create(QueryBuilder $queryBuilder): self
    {
        return new self($queryBuilder, null, null, null, false);
    }

    public function withMinimumSequenceNumber(SequenceNumber $sequenceNumber): self
    {
        if ($this->minimumSequenceNumber !== null && $sequenceNumber->value === $this->minimumSequenceNumber->value) {
            return $this;
        }
        return new self($this->queryBuilder, $sequenceNumber, $this->maximumSequenceNumber, $this->limit, $this->backwards);
    }

    public function withMaximumSequenceNumber(SequenceNumber $sequenceNumber): self
    {
        if ($this->maximumSequenceNumber !== null && $sequenceNumber->value === $this->maximumSequenceNumber->value) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $sequenceNumber, $this->limit, $this->backwards);
    }

    public function limit(int $limit): self
    {
        if ($limit === $this->limit) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $this->maximumSequenceNumber, $limit, $this->backwards);
    }

    public function backwards(): self
    {
        if ($this->backwards) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $this->maximumSequenceNumber, $this->limit, true);
    }

    public function getIterator(): \Traversable
    {
        $queryBuilder = clone $this->queryBuilder;
        if ($this->minimumSequenceNumber !== null) {
            $queryBuilder = $queryBuilder->andWhere('sequencenumber >= :minimumSequenceNumber')->setParameter('minimumSequenceNumber', $this->minimumSequenceNumber->value);
        }
        if ($this->maximumSequenceNumber !== null) {
            $queryBuilder = $queryBuilder->andWhere('sequencenumber <= :maximumSequenceNumber')->setParameter('maximumSequenceNumber', $this->maximumSequenceNumber->value);
        }
        if ($this->limit !== null) {
            $queryBuilder = $queryBuilder->setMaxResults($this->limit);
        }
        if ($this->backwards) {
            $queryBuilder = $queryBuilder->orderBy('sequencenumber', 'DESC');
        }

        $this->reconnectDatabaseConnection();

        /** @var Result<array<string, string>> $result */
        $result = $queryBuilder->execute();
        /** @var array<string, string> $row */
        foreach ($result->fetchAllAssociative() as $row) {
            $recordedAt = \DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['recordedat']);
            if ($recordedAt === false) {
                throw new \RuntimeException(sprintf('Failed to parse "recordetat" value of "%s" in event "%s"', $row['recordedat'], $row['id']), 1651744355);
            }
            yield new EventEnvelope(
                new Event(
                    EventId::fromString($row['id']),
                    EventType::fromString($row['type']),
                    EventData::fromString($row['payload']),
                    EventMetadata::fromJson($row['metadata']),
                ),
                StreamName::fromString($row['stream']),
                Version::fromInteger((int)$row['version']),
                SequenceNumber::fromInteger((int)$row['sequencenumber']),
                $recordedAt
            );
        }
    }

    // -----------------------------------

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->queryBuilder->getConnection()->fetchOne('SELECT 1');
        } catch (\Exception $_) {
            $this->queryBuilder->getConnection()->close();
            $this->queryBuilder->getConnection()->connect();
        }
    }
}
