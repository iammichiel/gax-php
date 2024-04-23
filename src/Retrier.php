<?php
/*
 * Copyright 2024 Google LLC
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
namespace Google\ApiCore\Middleware;

use Google\ApiCore\ApiException;
use Google\ApiCore\ApiStatus;
use Google\ApiCore\Call;
use Google\ApiCore\RetrySettings;

/**
 * Retrier functionality.
 */
class Retrier
{
    private RetrySettings $retrySettings;
    private ?float $deadlineMs;

    /*
     * The number of retries that have already been attempted.
     * The original API call will have $retryAttempts set to 0.
     */
    private int $retryAttempts;

    public function __construct(
        RetrySettings $retrySettings,
        $deadlineMs = null,
        $retryAttempts = 0
    ) {
        $this->retrySettings = $retrySettings;
        $this->deadlineMs = $deadlineMs;
        $this->retryAttempts = $retryAttempts;
    }

    /**
     * @param callable $call
     * @param array $options
     *
     * @return mixed
     * @throws Exception
     */
    public function execute(callable $call, array $options)
    {
        if (!isset($options['timeoutMillis'])) {
            // default to "noRetriesRpcTimeoutMillis" when retries are disabled, otherwise use "initialRpcTimeoutMillis"
            if (!$this->retrySettings->retriesEnabled() && $this->retrySettings->getNoRetriesRpcTimeoutMillis() > 0) {
                $options['timeoutMillis'] = $this->retrySettings->getNoRetriesRpcTimeoutMillis();
            } elseif ($this->retrySettings->getInitialRpcTimeoutMillis() > 0) {
                $options['timeoutMillis'] = $this->retrySettings->getInitialRpcTimeoutMillis();
            }
        }

        $continue = true;
        do {
            $retryDelayMillis = $this->retrySettings->getInitialRetryDelayMillis();
            try {
                return call_user_func_array($call, $options);
            } catch (\Exception $exception) {
                if ($this->retryFunction) {
                    // Throw the exception if retries are disabled.
                    if (!$this->retrySettings->retriesEnabled()) {
                        throw $exception;
                    }
                    $retryFunction = $this->getRetryFunction();

                    // If the number of retries has surpassed the max allowed retries
                    // then throw the exception as we normally would.
                    // If the maxRetries is set to 0, then we don't check this condition.
                    if (0 !== $this->retrySettings->getMaxRetries()
                        && $this->retryAttempts >= $this->retrySettings->getMaxRetries()
                    ) {
                        throw $e;
                    }
                    // If the retry function returns false then throw the
                    // exception as we normally would.
                    if (!$retryFunction($e, $options)) {
                        throw $e;
                    }

                    // Retry function returned true, so we attempt another retry
                    $options = $this->calculateRetryDelayAndTimeout($options, $exception);
                }
                usleep($retryDelayMillis * 1000);
                $this->retryAttempts++;
            }
        } while ($continue);
    }

    /**
     * @param array $options
     * @param \Exception $exception
     *
     * @return array
     * @throws Exception
     */
    private function calculateRetryDelayAndTimeout(array $options, \Exception $exception)
    {
        $maxTimeoutMs = $this->retrySettings->getMaxRpcTimeoutMillis();
        $timeoutMult = $this->retrySettings->getRpcTimeoutMultiplier();
        $totalTimeoutMs = $this->retrySettings->getTotalTimeoutMillis();
        $delayMs = $this->retrySettings->getRetryDelayMillis($this->retryAttempts, $exception);

        $timeoutMs = $options['timeoutMillis'];
        $currentTimeMs = $this->getCurrentTimeMs();
        $this->deadlineMs = $this->deadlineMs ?: $currentTimeMs + $totalTimeoutMs;

        if ($currentTimeMs >= $deadlineMs) {
            throw new ApiException(
                'Retry total timeout exceeded.',
                \Google\Rpc\Code::DEADLINE_EXCEEDED,
                ApiStatus::DEADLINE_EXCEEDED
            );
        }
        $timeoutMs = (int) min(
            $timeoutMs * $timeoutMult,
            $maxTimeoutMs,
            $deadlineMs - $this->getCurrentTimeMs()
        );
        $this->retrySettings = $this->retrySettings->with([
            'initialRetryDelayMillis' => $delayMs,
        ]);

        // Set the timeout for the call
        $options['timeoutMillis'] = $timeoutMs;
        return $options;
    }

    protected function getCurrentTimeMs()
    {
        return microtime(true) * 1000.0;
    }

    /**
     * This is the default retry behaviour.
     */
    private function getRetryFunction()
    {
        return $this->retrySettings->getRetryFunction() ??
            function (\Throwable $e, array $options): bool {
                // This is the default retry behaviour, i.e. we don't retry an ApiException
                // and for other exception types, we only retry when the error code is in
                // the list of retryable error codes.
                if (!$e instanceof ApiException) {
                    return false;
                }

                if (!in_array($e->getStatus(), $this->retrySettings->getRetryableCodes())) {
                    return false;
                }

                return true;
            };
    }
}
