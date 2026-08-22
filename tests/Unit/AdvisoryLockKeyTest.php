<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Tests\Unit;

use Neos\EventStore\DoctrineAdapter\Helper\AdvisoryLockKey;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

#[CoversClass(AdvisoryLockKey::class)]
class AdvisoryLockKeyTest extends TestCase
{
    #[Test]
    public function flyweight(): void
    {
        self::assertSame(
            AdvisoryLockKey::fromTableName('events_test'),
            AdvisoryLockKey::fromTableName('events_test')
        );

        self::assertNotEquals(
            AdvisoryLockKey::fromTableName('a'),
            AdvisoryLockKey::fromTableName('b')
        );
    }

    #[Test]
    public function fromTableName(): void
    {
        $lockKey = AdvisoryLockKey::fromTableName('events_test');
        self::assertSame('11c4d468d7370ba7', $lockKey->as16CharHexString());
        self::assertSame(1280381740832459687, $lockKey->as64BitInt());

        $lockKey = AdvisoryLockKey::fromTableName('cr_default_events');
        self::assertSame('e8c449e26a8a2bb3', $lockKey->as16CharHexString());
        self::assertSame(-1674131924676105293, $lockKey->as64BitInt());
    }
}
