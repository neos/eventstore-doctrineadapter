<?php

declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Helper;

/**
 * @internal
 */
final readonly class AdvisoryLockKey
{
    private function __construct(
        private string $binaryHash
    ) {
    }

    public static function fromString(string $string): self
    {
        return new self(
            binaryHash: md5($string, binary: true)
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
