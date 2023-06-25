<?php
namespace Imefisto\EventSaucePDOMessageRepository\Testing;

use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\Serialization\ConstructingMessageSerializer;
use EventSauce\EventSourcing\Serialization\MySQL8DateFormatting;
use EventSauce\MessageRepository\TestTooling\MessageRepositoryTestCase;
use Imefisto\EventSaucePDOMessageRepository\DefaultConnectionManager;
use Imefisto\EventSaucePDOMessageRepository\DummyAggregateRootId;
use Imefisto\EventSaucePDOMessageRepository\PDOMessageRepository;
use Ramsey\Uuid\Uuid;

/**
 * @covers PDOMessageRepository
 */
class PDOMessageRepositoryTest extends MessageRepositoryTestCase
{
    private string $dsn = 'mysql:host=127.0.0.1;port=33060;dbname=basket'; 
    private string $user = 'test';
    private string $password = 'test';

    protected function setUp(): void
    {
        parent::setUp();

        $pdo = new \PDO(
            $this->dsn,
            $this->user,
            $this->password
        );

        $stmt = $pdo->prepare('TRUNCATE TABLE domain_messages_uuid');
        $stmt->execute();
    }

    protected function messageRepository(): MessageRepository
    {
        $connection = new DefaultConnectionManager(
            $this->dsn,
            $this->user,
            $this->password
        );

        return new PDOMessageRepository(
            $connection,
            $this->tableName,
            new MySQL8DateFormatting(new ConstructingMessageSerializer())
        );
    }

    protected function aggregateRootId(): AggregateRootId
    {
        return DummyAggregateRootId::generate();
    }

    protected function eventId(): string
    {
        return Uuid::uuid7()->toString();
    }
}
