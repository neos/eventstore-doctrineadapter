<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Neos\EventStore\DoctrineAdapter\DoctrineCheckpointStorage;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Tests\Integration\AbstractCheckpointStorageTestBase;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(DoctrineCheckpointStorage::class)]
final class DoctrineCheckpointStorageTest extends AbstractCheckpointStorageTestBase
{
    private static string $dbDns;

    /** @var array<array{storage: DoctrineCheckpointStorage, connection: Connection}> */
    private array $storages = [];

    public static function setUpBeforeClass(): void
    {
        $dbDns = getenv('DB_DSN');
        if ($dbDns === false) {
            self::markTestSkipped('Missing DB_DSN environment variable, see https://phpunit.readthedocs.io/en/9.5/configuration.html#the-env-element');
        }
        self::$dbDns = $dbDns;
    }

    public static function tearDownAfterClass(): void
    {
        DriverManager::getConnection(['url' => self::$dbDns])->executeStatement('DROP TABLE IF EXISTS neos_eventstore_doctrineadapter_doctrinecheckpointstoragetest');
    }

    public function tearDown(): void
    {
        /** @var array{storage: DoctrineCheckpointStorage, connection: Connection} $storage */
        foreach ($this->storages as $storage) {
            if (!$storage['connection']->isTransactionActive()) {
                $storage['storage']->acquireLock();
            }
            $storage['storage']->updateAndReleaseLock(SequenceNumber::none());
        }
    }

    protected function createCheckpointStorage(string $subscriptionId): DoctrineCheckpointStorage
    {
        $connection = DriverManager::getConnection(['url' => self::$dbDns]);
        $checkpointStorage = new DoctrineCheckpointStorage($connection, 'neos_eventstore_doctrineadapter_doctrinecheckpointstoragetest', $subscriptionId);
        $checkpointStorage->setup();
        $this->storages[] = ['connection' => $connection, 'storage' => $checkpointStorage];
        return $checkpointStorage;
    }
}
