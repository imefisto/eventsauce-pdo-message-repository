<?php
namespace Imefisto\EventSaucePDOMessageRepository\Testing;

use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\MessageRepository\TestTooling\MessageRepositoryTestCase;
use Imefisto\EventSaucePDOMessageRepository\DummyAggregateRootId;
use Imefisto\EventSaucePDOMessageRepository\PDOMessageRepository;
use Ramsey\Uuid\Uuid;

class PDOMessageRepositoryTest extends MessageRepositoryTestCase
{
    protected function messageRepository(): MessageRepository
    {
        return new PDOMessageRepository;
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
