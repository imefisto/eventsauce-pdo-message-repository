<?php
namespace Imefisto\EventSaucePDOMessageRepository;

class DefaultConnectionManager implements ConnectionManager
{
    private $pdo;
    private array $options;

    public function __construct(
        private string $dsn,
        private string $user,
        private string $password,
        ?array $options = []
    ) {
        $this->options = !empty($options)
            ? $options
            : [ \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION ]
            ;
    }

    public function get(): \PDO
    {
        if (empty($this->pdo)) {
            $this->reconnect();
        }

        $pdo = $this->pdo;
        $this->pdo = null;

        return $pdo;
    }

    private function reconnect()
    {
        $this->pdo = new \PDO(
            $this->dsn,
            $this->user,
            $this->password,
            $this->options
        );

        return $this->pdo;
    }

    public function put($pdo): void
    {
        $this->pdo = $pdo;
    }
}
