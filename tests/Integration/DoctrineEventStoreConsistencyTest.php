<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\Tests\Integration\AbstractEventStoreConsistencyTestBase;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(DoctrineEventStore::class)]
final class DoctrineEventStoreConsistencyTest extends AbstractEventStoreConsistencyTestBase
{
    use DoctrineEventStoreTrait;
}
