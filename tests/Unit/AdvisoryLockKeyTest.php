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
        $lockKey = AdvisoryLockKey::fromTableName('events_test');
        self::assertSame('11c4d468d7370ba7', $lockKey->as16CharString());
        self::assertSame(1280381740832459687, $lockKey->as64BitInt());
    }
}
