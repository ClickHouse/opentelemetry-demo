<?php
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use Psr\Http\Message\ResponseInterface as Response;
use Psr\Http\Message\ServerRequestInterface as Request;
use Psr\Log\LoggerInterface;
use Slim\App;

function calculateQuote($jsonObject): float
{
    if (!array_key_exists('numberOfItems', $jsonObject)) {
        throw new \InvalidArgumentException('numberOfItems not provided');
    }
    $numberOfItems = intval($jsonObject['numberOfItems']);
    $costPerItem = rand(400, 1000)/10;
    $quote = round($costPerItem * $numberOfItems, 2);
    return $quote;
}

return function (App $app) {
    $app->post('/getquote', function (Request $request, Response $response, LoggerInterface $logger) {
        $jsonObject = $request->getParsedBody();
        $data = calculateQuote($jsonObject);
        $payload = json_encode($data);
        $response->getBody()->write($payload);
        $logger->info('Calculated quote', [
            'total' => $data,
        ]);
        return $response
            ->withHeader('Content-Type', 'application/json');
    });
};
