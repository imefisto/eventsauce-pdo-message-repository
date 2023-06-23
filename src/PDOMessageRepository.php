<?php
namespace Imefisto\EventSaucePDOMessageRepository;

use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\PaginationCursor;

class PDOMessageRepository implements MessageRepository
{
    public function persist(Message ...$messages): void
    {
    }

    public function retrieveAll(AggregateRootId $id): \Generator
    {
    }

    public function retrieveAllAfterVersion(
        AggregateRootId $id,
        int $aggregateRootVersion
    ): \Generator {
    }

    public function paginate(PaginationCursor $cursor): \Generator
    {
    }
}
