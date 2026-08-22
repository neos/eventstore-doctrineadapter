<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Helper;

/**
 * @internal
 */
final class AdvisoryLockKey
{
    /**
     * @var array<string,self>
     */
    private static array $instances = [];

    private function __construct(
        private readonly string $binaryHash
    ) {
    }

    public static function fromTableName(string $tableName): self
    {
        return self::$instances[$tableName] ??= new self(
            binaryHash: md5($tableName, binary: true)
        );
    }

    public function as16CharHexString(): string
    {
        /** @var array{1: string} $u */
        $u = unpack('H16', $this->binaryHash);
        return $u[1];
    }

    public function as64BitInt(): int
    {
        /** @var array{1: int} $u */
        $u = unpack('J1', $this->binaryHash);
        return $u[1];
    }
}
