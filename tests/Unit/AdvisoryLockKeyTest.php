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
    public function fromTableName(): void
    {
        $lockKey = AdvisoryLockKey::fromString('');
        self::assertSame('d41d8cd98f00b204', $lockKey->as16CharHexString());
        self::assertSame(-3162216497309240828, $lockKey->as64BitInt());

        $lockKey = AdvisoryLockKey::fromString('events_test');
        self::assertSame('11c4d468d7370ba7', $lockKey->as16CharHexString());
        self::assertSame(1280381740832459687, $lockKey->as64BitInt());

        $lockKey = AdvisoryLockKey::fromString('cr_default_events');
        self::assertSame('e8c449e26a8a2bb3', $lockKey->as16CharHexString());
        self::assertSame(-1674131924676105293, $lockKey->as64BitInt());

        $lockKey = AdvisoryLockKey::fromString('my-database.cr_default_events');
        self::assertSame('ba96072c16045a06', $lockKey->as16CharHexString());
        self::assertSame(-5001802450219017722, $lockKey->as64BitInt());
    }
}
