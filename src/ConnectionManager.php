<?php
namespace Imefisto\EventSaucePDOMessageRepository;

interface ConnectionManager
{
    public function get(): object;
    public function put(object $conn): void;
}
