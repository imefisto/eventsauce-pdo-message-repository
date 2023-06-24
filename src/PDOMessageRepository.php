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
use EventSauce\MessageRepository\TableSchema\DefaultTableSchema;
use Ramsey\Uuid\Uuid;

class PDOMessageRepository implements MessageRepository
{
    public function __construct(
        private string $dsn,
        private string $user,
        private string $password,
        private string $tableName,
        private MessageSerializer $serializer,
        private int $jsonEncodeOptions = 0,
        ?TableSchema $tableSchema = null,
        ?IdEncoder $aggregateRootIdEncoder = null,
        ?IdEncoder $eventIdEncoder = null,
        ?array $pdoOptions = []
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

            $stmt = $this->connection()->prepare($sql);
            $stmt->execute($values);
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
            $stmt = $this->connection()->prepare($sql);
            $stmt->execute(
                [
                    $this->aggregateRootIdEncoder->encodeId($id)
                ]
            );

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
            $stmt = $this->connection()->prepare($sql);
            $stmt->execute(
                [
                    $this->aggregateRootIdEncoder->encodeId($id),
                    $aggregateRootVersion
                ]
            );

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
            $stmt = $this->connection()->prepare($sql);
            $stmt->bindParam(':offset', $offset, \PDO::PARAM_INT);
            $stmt->bindParam(':limit', $limit, \PDO::PARAM_INT);
            $stmt->execute();

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

    private function connection()
    {
        if (empty($this->pdo)) {
            $this->_reconnect();
        }

        return $this->pdo;
    }

    private function _reconnect()
    {
        $defaultOptions = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ];

        $this->pdo = new \PDO(
            $this->dsn,
            $this->user,
            $this->password,
            array_merge(
                $defaultOptions,
                $this->pdoOptions ?? []
            )
        );

        return $this->pdo;
    }
}
