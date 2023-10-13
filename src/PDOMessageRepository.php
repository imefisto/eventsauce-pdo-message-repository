<?php
namespace Imefisto\EventSaucePDOMessageRepository;

use EventSauce\EventSourcing\AggregateRootId;
use EventSauce\EventSourcing\Header;
use EventSauce\EventSourcing\Message;
use EventSauce\EventSourcing\MessageRepository;
use EventSauce\EventSourcing\OffsetCursor;
use EventSauce\EventSourcing\PaginationCursor;
use EventSauce\EventSourcing\Serialization\MessageSerializer;
use EventSauce\EventSourcing\UnableToPersistMessages;
use EventSauce\EventSourcing\UnableToRetrieveMessages;
use EventSauce\IdEncoding\BinaryUuidIdEncoder;
use EventSauce\IdEncoding\IdEncoder;
use EventSauce\MessageRepository\TableSchema\DefaultTableSchema;
use EventSauce\MessageRepository\TableSchema\TableSchema;
use Ramsey\Uuid\Uuid;

class PDOMessageRepository implements MessageRepository
{
    private TableSchema $tableSchema;
    private IdEncoder $aggregateRootIdEncoder;
    private IdEncoder $eventIdEncoder;

    public function __construct(
        private ConnectionManager $connection,
        private string $tableName,
        private MessageSerializer $serializer,
        private int $jsonEncodeOptions = 0,
        ?TableSchema $tableSchema = null,
        ?IdEncoder $aggregateRootIdEncoder = null,
        ?IdEncoder $eventIdEncoder = null
    ) {
        $this->tableSchema = $tableSchema ?? new DefaultTableSchema();
        $this->aggregateRootIdEncoder = $aggregateRootIdEncoder
            ?? new BinaryUuidIdEncoder();
        $this->eventIdEncoder = $eventIdEncoder ?? $this->aggregateRootIdEncoder;
    }
    
    public function persist(Message ...$messages): void
    {
        if (count($messages) === 0) {
            return;
        }

        $additionalColumns = $this->tableSchema->additionalColumns();

        $columns = array_merge(
            [
                $this->tableSchema->versionColumn(),
                $this->tableSchema->eventIdColumn(),
                $this->tableSchema->payloadColumn(),
                $this->tableSchema->aggregateRootIdColumn(),
            ],
            array_keys($additionalColumns)
        );

        $placeholders = '('
            . implode(',', array_fill(0, count($columns), '?'))
            . ')';

        $placeholdersForFields = $placeholders;
        $placeholdersForValues = [];
        $values = [];

        foreach ($messages as $message) {
            $payload = $this->serializer->serializeMessage($message);
            $payload['headers'][Header::EVENT_ID]
                = $payload['headers'][Header::EVENT_ID]
                ?? Uuid::uuid7()->toString();

            $parameters = [
                $payload['headers'][Header::AGGREGATE_ROOT_VERSION] ?? 0,
                $this->eventIdEncoder->encodeId(
                    $payload['headers'][Header::EVENT_ID]
                ),
                json_encode($payload, $this->jsonEncodeOptions),
                $this->aggregateRootIdEncoder->encodeId($message->aggregateRootId()),
            ];

            foreach ($additionalColumns as $column => $header) {
                $parameters[] = $payload['headers'][$header];
            }

            $placeholdersForValues[] = $placeholders;
            $values = array_merge(
                $values,
                $parameters
            );
        }

        try {
            $sql = 'INSERT INTO ' . $this->tableName
                . '(' . implode(',', $columns) . ')'
                . 'VALUES '
                . implode(',', $placeholdersForValues);

            $conn = $this->connection->get();
            $stmt = $conn->prepare($sql);
            $stmt->execute($values);
            $this->connection->put($conn);
        } catch (\PDOException $exception) {
            throw UnableToPersistMessages::dueTo('', $exception);
        }
    }

    public function retrieveAll(AggregateRootId $id): \Generator
    {
        $sql = 'SELECT payload FROM ' . $this->tableName
            . ' WHERE ' . $this->tableSchema->aggregateRootIdColumn() . ' = ?'
            . ' ORDER BY ' . $this->tableSchema->versionColumn() . ' ASC';

        try {
            $conn = $this->connection->get();
            $stmt = $conn->prepare($sql);
            $stmt->execute(
                [
                    $this->aggregateRootIdEncoder->encodeId($id)
                ]
            );
            $this->connection->put($conn);

            return $this->yieldMessagesForResult($stmt);
        } catch (\PDOException $exception) {
            throw UnableToRetrieveMessages::dueTo('', $exception);
        }
    }

    public function retrieveAllAfterVersion(
        AggregateRootId $id,
        int $aggregateRootVersion
    ): \Generator {
        $sql = 'SELECT payload FROM ' . $this->tableName
            . ' WHERE ' . $this->tableSchema->aggregateRootIdColumn() . ' = ?'
            . ' AND ' . $this->tableSchema->versionColumn() . ' > ?'
            . ' ORDER BY ' . $this->tableSchema->versionColumn() . ' ASC';

        try {
            $conn = $this->connection->get();
            $stmt = $conn->prepare($sql);
            $stmt->execute(
                [
                    $this->aggregateRootIdEncoder->encodeId($id),
                    $aggregateRootVersion
                ]
            );
            $this->connection->put($conn);

            return $this->yieldMessagesForResult($stmt);
        } catch (\PDOException $exception) {
            throw UnableToRetrieveMessages::dueTo('', $exception);
        }
    }

    private function yieldMessagesForResult(\PDOStatement $stmt): \Generator
    {
        while ($row = $stmt->fetch(\PDO::FETCH_OBJ)) {
            $payload = json_decode($row->payload, true);
            yield $this->serializer->unserializePayload(
                $payload
            );
        }

        return isset($message)
            ? $message->header(Header::AGGREGATE_ROOT_VERSION) ?: 0
            : 0;
    }

    public function paginate(PaginationCursor $cursor): \Generator
    {
        if (! $cursor instanceof OffsetCursor) {
            throw new LogicException(
                sprintf(
                    'Wrong cursor type used, expected %s, received %s',
                    OffsetCursor::class,
                    get_class($cursor)
                )
            );
        }

        $offset = $cursor->offset();
        $limit = $cursor->limit();
        $incrementalIdColumn = $this->tableSchema->incrementalIdColumn();

        $sql = 'SELECT payload FROM ' . $this->tableName
            . ' WHERE ' . $incrementalIdColumn . ' > :offset'
            . ' ORDER BY ' . $incrementalIdColumn . ' ASC'
            . ' LIMIT :limit';

        try {
            $conn = $this->connection->get();
            $stmt = $conn->prepare($sql);
            $stmt->bindParam(':offset', $offset, \PDO::PARAM_INT);
            $stmt->bindParam(':limit', $limit, \PDO::PARAM_INT);
            $stmt->execute();
            $this->connection->put($conn);

            while ($row = $stmt->fetch(\PDO::FETCH_OBJ)) {
                $offset ++;
                $payload = json_decode($row->payload, true);
                yield $this->serializer->unserializePayload(
                    $payload
                );
            }

            return $cursor->withOffset($offset);
        } catch (\PDOException $exception) {
            throw UnableToRetrieveMessages::dueTo('', $exception);
        }
    }
}
