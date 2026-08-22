<?php
declare(strict_types=1);

namespace Neos\EventStore\DoctrineAdapter\Helper;

final readonly class AdvisoryLockKey
{
    private function __construct(
        private string $value
    ) {
    }

    public static function fromTableName(string $tableName): self
    {
        $hash32Chars = md5($tableName);
        return new self(
            substr($hash32Chars, 0, 16)
        );
    }

    public function as16CharString(): string
    {
        return $this->value;
    }

    public function as64BitInt(): int
    {
        return hexdec($this->value);
    }
}
