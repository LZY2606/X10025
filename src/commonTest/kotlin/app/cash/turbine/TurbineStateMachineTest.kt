/*
 * Copyright (C) 2025 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package app.cash.turbine

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext

/**
 * Replayable state-machine regressions for three interacting mechanisms:
 *
 * - Turbine timeouts (the virtual-time `withTimeout` branch and the scheduler-aware wall-clock
 *   fallback selected inside `withAppropriateTimeout`).
 * - Cancellation propagation (parent scope cancellation, `cancel()` and
 *   `cancelAndIgnoreRemainingEvents()`).
 * - Unconsumed terminal event accounting performed by `ChannelTurbine.reportUnconsumedEvents` on
 *   explicit `ensureAllEventsConsumed()`, at the end of `test {}`, and when a `testIn` scope exits.
 *
 * No test in this file waits on the wall clock. Cases that exercise timeout firing construct a
 * scope whose dispatcher is driven by a [TestCoroutineScheduler] but which deliberately omits the
 * scheduler element from its coroutine context, so Turbine selects the `withTimeout` branch which
 * is fully virtual-time capable. Boundary matrices are enumerated exhaustively (no randomness), and
 * each matrix repeats [replayCount] times to demonstrate that there is no timing jitter.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class TurbineStateMachineTest {
  private val replayCount = 20

  // region Virtual time and timeout

  /**
   * A scope backed by a scheduler-controlled dispatcher but without the scheduler context element.
   *
   * `withAppropriateTimeout` branches on `coroutineContext[TestCoroutineScheduler] != null`: a
   * `runTest` body selects the wall-clock fallback (the virtual clock would hang it forever),
   * whereas this scope selects the regular [kotlinx.coroutines.withTimeout] whose delay is
   * scheduled by [scheduler]. The timeout therefore fires deterministically in zero real time.
   */
  private fun virtualTimeScope(scheduler: TestCoroutineScheduler): CoroutineScope =
    CoroutineScope(StandardTestDispatcher(scheduler))

  private fun <T> TestCoroutineScheduler.virtualAsync(
    start: CoroutineStart = CoroutineStart.UNDISPATCHED,
    block: suspend () -> T,
  ): Deferred<T> = virtualTimeScope(this).async(start = start) { block() }

  @Test
  fun virtualTimeoutFiresWithTimeoutCauseAndNoRealWait() = runTest {
    val totalMark = TimeSource.Monotonic.markNow()
    repeat(replayCount) {
      val mark = TimeSource.Monotonic.markNow()
      val turbine = Turbine<Unit>(timeout = 1.hours)

      val awaiter = testScheduler.virtualAsync { turbine.awaitItem() }
      advanceUntilIdle()

      val error = awaiter.getCompletionExceptionOrNull()
      assertNotNull(error)
      assertTrue(error is AssertionError, "expected AssertionError but was $error")
      assertEquals("No value produced in 1h", error.message)
      assertTrue(
        error.cause is TimeoutCancellationException,
        "expected the TimeoutCancellationException to be retained as cause, was ${error.cause}",
      )
      assertTrue(
        mark.elapsedNow() < 500.milliseconds,
        "a virtual timeout must not consume wall-clock time, took ${mark.elapsedNow()}",
      )
    }
    assertTrue(
      totalMark.elapsedNow() < 2.seconds,
      "replaying $replayCount virtual timeouts took ${totalMark.elapsedNow()}",
    )
  }

  @Test
  fun bufferedItemBeatsTimeoutWithoutRealWait() = runTest {
    repeat(replayCount) {
      val mark = TimeSource.Monotonic.markNow()
      val turbine = Turbine<Int>(timeout = 1.hours)
      turbine.add(7)

      val awaiter = testScheduler.virtualAsync { turbine.awaitItem() }
      advanceUntilIdle()

      assertEquals(7, awaiter.getCompleted())
      assertTrue(
        mark.elapsedNow() < 500.milliseconds,
        "buffered event must resolve immediately, took ${mark.elapsedNow()}",
      )
    }
  }

  @Test
  fun lateVirtualItemArrivingBeforeDeadlineBeatsTimeout() = runTest {
    val turbine = Turbine<String>(timeout = 1.hours)
    val scope = virtualTimeScope(testScheduler)
    val producer = scope.async {
      delay(30.minutes)
      turbine.add("late")
    }
    val consumer = scope.async(start = CoroutineStart.UNDISPATCHED) { turbine.awaitItem() }

    advanceUntilIdle()

    assertEquals("late", consumer.getCompleted())
    producer.cancel()
  }

  @Test
  fun terminalErrorAlreadyBufferedBeatsTimeoutAndPreservesCause() = runTest {
    val expected = CustomThrowable("upstream boom")
    val turbine = Turbine<Unit>(timeout = 1.hours)
    turbine.close(expected)

    val awaiter = testScheduler.virtualAsync { turbine.awaitItem() }
    advanceUntilIdle()

    val error = awaiter.getCompletionExceptionOrNull()
    assertNotNull(error)
    assertTrue(error is AssertionError, "expected AssertionError but was $error")
    assertEquals("Expected item but found Error(CustomThrowable)", error.message)
    assertSame(expected, error.cause)
  }

  @Test
  fun turbineRemainsReadableAfterTimeout() = runTest {
    val turbine = Turbine<Int>(timeout = 1.hours)

    val first = testScheduler.virtualAsync { turbine.awaitItem() }
    advanceUntilIdle()
    assertNotNull(first.getCompletionExceptionOrNull(), "await must have timed out")

    turbine.add(1)
    turbine.close()

    val second = testScheduler.virtualAsync { turbine.awaitItem() }
    advanceUntilIdle()
    assertEquals(1, second.getCompleted())

    val completion = testScheduler.virtualAsync { turbine.awaitComplete() }
    advanceUntilIdle()
    assertNull(completion.getCompletionExceptionOrNull())
  }

  @Test
  fun timeoutFailureKeepsDiagnosticName() = runTest {
    val turbine = Turbine<Unit>(timeout = 1.hours, name = "named virtual turbine")

    val awaiter = testScheduler.virtualAsync { turbine.awaitItem() }
    advanceUntilIdle()

    val error = awaiter.getCompletionExceptionOrNull()
    assertNotNull(error)
    assertEquals("No value produced for named virtual turbine in 1h", error.message)
  }

  // endregion

  // region Parent coroutine cancellation

  private class ParentCancellation : CancellationException("parent cancelled")

  @Test
  fun parentCancellationDuringAwaitPropagatesCancellationNotTimeout() = runTest {
    repeat(replayCount) {
      val gate = CompletableDeferred<Unit>()
      val observed = CompletableDeferred<Throwable?>()
      val turbine = Turbine<Unit>()

      val parent =
        launch(start = CoroutineStart.UNDISPATCHED) {
          launch {
            gate.complete(Unit)
            try {
              turbine.awaitItem()
              observed.complete(null)
            } catch (throwable: Throwable) {
              observed.complete(throwable)
            }
          }
        }

      gate.await()
      parent.cancel(ParentCancellation())
      advanceUntilIdle()

      val thrown = observed.getCompleted()
      assertNotNull(thrown)
      assertTrue(
        thrown is ParentCancellation,
        "parent cancellation must surface as cancellation, was $thrown",
      )
    }
  }

  private enum class BufferedTerminal {
    NONE,
    ITEMS,
    COMPLETE,
    ERROR,
    CANCELLATION_ERROR,
  }

  @Test
  fun externalParentCancelReportsExactlyTheBufferedEventsBoundaryMatrix() = runTest {
    for (terminal in BufferedTerminal.entries) {
      val handler = RecordingExceptionHandler()
      withContext(handler) {
        turbineScope {
          val source =
            flow<String> {
              when (terminal) {
                BufferedTerminal.NONE -> Unit
                BufferedTerminal.ITEMS -> {
                  emit("one")
                  emit("two")
                }
                BufferedTerminal.COMPLETE -> emit("one")
                BufferedTerminal.ERROR -> {
                  emit("one")
                  throw CustomThrowable("terminal")
                }
                BufferedTerminal.CANCELLATION_ERROR -> {
                  emit("one")
                  throw CancellationException("upstream cancelled itself")
                }
              }
              if (terminal != BufferedTerminal.COMPLETE) emitAll(neverFlow())
            }

          val turbine = source.testIn(this)
          advanceUntilIdle()
          turbine.cancel()
        }
      }
      advanceUntilIdle()

      val failure = handler.exceptions.singleOrNull()
      when (terminal) {
        BufferedTerminal.NONE -> assertNull(failure, "no buffered events must be reported")
        BufferedTerminal.ITEMS ->
          assertEquals(
            """
            |Unconsumed events found:
            | - Item(one)
            | - Item(two)
            """
              .trimMargin(),
            failure?.cause?.message,
          )
        BufferedTerminal.COMPLETE ->
          // The flow finished naturally before cancel(); a terminal event already delivered
          // cannot retroactively be suppressed, so both the item and completion are reported.
          assertEquals(
            """
            |Unconsumed events found:
            | - Item(one)
            | - Complete
            """
              .trimMargin(),
            failure?.cause?.message,
          )
        BufferedTerminal.ERROR -> {
          assertEquals(
            """
            |Unconsumed events found:
            | - Item(one)
            | - Error(CustomThrowable)
            """
              .trimMargin(),
            failure?.cause?.message,
          )
          val assertion = failure?.cause
          assertTrue(assertion is AssertionError)
          assertTrue(assertion.cause is CustomThrowable)
        }
        BufferedTerminal.CANCELLATION_ERROR ->
          // Cancellation terminations from upstream are stripped: they are not meaningful test
          // feedback while the hierarchy is being torn down.
          assertEquals(
            """
            |Unconsumed events found:
            | - Item(one)
            """
              .trimMargin(),
            failure?.cause?.message,
          )
      }
    }
  }

  // endregion

  // region Upstream exceptions

  @Test
  fun upstreamErrorBeatsTimeoutAttemptAndPreservesCause() = runTest {
    val expected = CustomThrowable("slow failure")
    val turbine = Turbine<Int>(timeout = 1.hours)
    turbine.add(1)
    turbine.close(expected)

    val item = testScheduler.virtualAsync { turbine.awaitItem() }
    advanceUntilIdle()
    assertEquals(1, item.getCompleted())

    val awaiter = testScheduler.virtualAsync { turbine.awaitComplete() }
    advanceUntilIdle()

    val error = awaiter.getCompletionExceptionOrNull()
    assertNotNull(error)
    assertTrue(error is AssertionError)
    assertEquals("Expected complete but found Error(CustomThrowable)", error.message)
    assertSame(expected, error.cause)
  }

  @Test
  fun upstreamTerminalErrorReturnsSameThrowableOnEveryReRead() = runTest {
    val expected = CustomThrowable("failure after item")
    val turbine = Turbine<Int>(timeout = 1.hours)
    turbine.add(1)
    turbine.close(expected)

    val item = testScheduler.virtualAsync { turbine.awaitItem() }
    advanceUntilIdle()
    assertEquals(1, item.getCompleted())

    repeat(replayCount) { readIndex ->
      val probe = testScheduler.virtualAsync {
        Triple(
          runCatching { turbine.awaitItem() }.exceptionOrNull(),
          runCatching { turbine.awaitComplete() }.exceptionOrNull(),
          runCatching { turbine.awaitError() }.getOrNull(),
        )
      }
      advanceUntilIdle()

      val (fromItem, fromComplete, fromError) = probe.getCompleted()
      assertSame(expected, fromError, "read $readIndex: awaitError must return the throwable")
      assertSame(expected, fromItem?.cause, "read $readIndex: awaitItem cause")
      assertSame(expected, fromComplete?.cause, "read $readIndex: awaitComplete cause")
    }
  }

  // endregion

  // region Repeated cancellation

  @Test
  fun repeatedCancelBeforeTerminalIsIdempotentAndSuppressesTerminal() = runTest {
    repeat(replayCount) {
      val turbine = Turbine<String>()
      turbine.add("a")
      turbine.cancel()
      turbine.cancel()

      val failure = assertFailsWith<AssertionError> { turbine.ensureAllEventsConsumed() }
      assertEquals(
        """
        |Unconsumed events found:
        | - Item(a)
        """
          .trimMargin(),
        failure.message,
      )
    }
  }

  @Test
  fun cancelAfterNaturalCompletionDoesNotSuppressTerminalEvent() = runTest {
    repeat(replayCount) {
      val handler = RecordingExceptionHandler()
      withContext(handler) {
        turbineScope {
          val turbine = emptyFlow<String>().testIn(this)
          advanceUntilIdle()
          turbine.cancel()
        }
      }
      advanceUntilIdle()
      assertEquals(
        """
        |Unconsumed events found:
        | - Complete
        """
          .trimMargin(),
        handler.exceptions.singleOrNull()?.cause?.message,
      )
    }
  }

  @Test
  fun cancelAndIgnoreRemainingEventsIsIdempotent() = runTest {
    repeat(replayCount) {
      turbineScope {
        val turbine = flow {
          emit("one")
          emit("two")
          emitAll(neverFlow())
        }
          .testIn(this)
        advanceUntilIdle()
        turbine.cancelAndIgnoreRemainingEvents()
        turbine.cancelAndIgnoreRemainingEvents()
        turbine.ensureAllEventsConsumed()
      }
    }
  }

  @Test
  fun repeatedConsumingCancelAfterNaturalCompletionIsStable() = runTest {
    val turbine = Turbine<String>()
    turbine.close()

    val first = turbine.cancelAndConsumeRemainingEvents()
    assertEquals(listOf(Event.Complete), first)

    // Regression for an asymmetry worth pinning: the terminal event lives in the closed channel
    // and is not consumed away, so draining a second time reports the same terminal again.
    val second = turbine.cancelAndConsumeRemainingEvents()
    assertEquals(listOf(Event.Complete), second)
  }

  // endregion

  // region Misreads after a terminal state (generative property)

  private enum class TerminalKind {
    COMPLETE,
    ERROR,
  }

  private enum class ReaderKind {
    AWAIT_ITEM,
    AWAIT_COMPLETE,
    AWAIT_ERROR,
    AWAIT_EVENT,
  }

  @Test
  fun terminalStateIsStableAcrossEveryReaderBoundaryMatrix() = runTest {
    for (terminal in TerminalKind.entries) {
      for (reader in ReaderKind.entries) {
        repeat(replayCount) { readIndex ->
          val terminalError = CustomThrowable("terminal $terminal/$reader/$readIndex")
          val turbine = Turbine<String>()
          if (terminal == TerminalKind.COMPLETE) {
            turbine.close()
          } else {
            turbine.close(terminalError)
          }

          repeat(2) { pass ->
            val probe = testScheduler.virtualAsync {
              when (reader) {
                ReaderKind.AWAIT_ITEM -> runCatching { turbine.awaitItem() as Any? }
                ReaderKind.AWAIT_COMPLETE -> runCatching { turbine.awaitComplete() as Any? }
                ReaderKind.AWAIT_ERROR -> runCatching { turbine.awaitError() as Any? }
                ReaderKind.AWAIT_EVENT -> runCatching { turbine.awaitEvent() as Any? }
              }
            }
            advanceUntilIdle()

            val result = probe.getCompleted()
            assertReaderExpectation(reader, terminal, terminalError, result)
          }
        }
      }
    }
  }

  private fun assertReaderExpectation(
    reader: ReaderKind,
    terminal: TerminalKind,
    terminalError: CustomThrowable,
    result: Result<Any?>,
  ) {
    when (reader) {
      ReaderKind.AWAIT_ITEM -> {
        val failure = result.exceptionOrNull()
        assertTrue(failure is AssertionError, "expected AssertionError, was $failure")
        when (terminal) {
          TerminalKind.COMPLETE -> assertEquals("Expected item but found Complete", failure.message)
          TerminalKind.ERROR -> {
            assertEquals("Expected item but found Error(CustomThrowable)", failure.message)
            assertSame(terminalError, failure.cause)
          }
        }
      }
      ReaderKind.AWAIT_COMPLETE ->
        when (terminal) {
          TerminalKind.COMPLETE -> assertEquals(Unit, result.getOrNull())
          TerminalKind.ERROR -> {
            val failure = result.exceptionOrNull()
            assertTrue(failure is AssertionError)
            assertEquals("Expected complete but found Error(CustomThrowable)", failure.message)
            assertSame(terminalError, failure.cause)
          }
        }
      ReaderKind.AWAIT_ERROR ->
        when (terminal) {
          TerminalKind.COMPLETE -> {
            val failure = result.exceptionOrNull()
            assertTrue(failure is AssertionError)
            assertEquals("Expected error but found Complete", failure.message)
          }
          TerminalKind.ERROR -> assertSame(terminalError, result.getOrNull())
        }
      ReaderKind.AWAIT_EVENT ->
        when (terminal) {
          TerminalKind.COMPLETE -> assertEquals(Event.Complete, result.getOrNull())
          TerminalKind.ERROR -> assertEquals(Event.Error(terminalError), result.getOrNull())
        }
    }
  }

  @Test
  fun failedReadStillAdvancesTheChannelAndTerminalThenIsSticky() = runTest {
    repeat(replayCount) {
      val terminalError = CustomThrowable("late terminal")
      val turbine = Turbine<String>()
      turbine.add("v")
      turbine.close(terminalError)

      // A failed assertion does not leave the stream untouched: expectNoEvents reads the head
      // through tryReceive, reports it as unexpected, and that Item(v) is consumed away.
      val itemFirst = assertFailsWith<AssertionError> { turbine.expectNoEvents() }
      assertEquals("Expected no events but found Item(v)", itemFirst.message)

      // Consequently the terminal Error is already at the head and is reported for every
      // mismatched reader...
      val expectingComplete = testScheduler.virtualAsync { turbine.awaitComplete() }
      advanceUntilIdle()
      val completeFailure = expectingComplete.getCompletionExceptionOrNull()
      assertNotNull(completeFailure)
      assertEquals("Expected complete but found Error(CustomThrowable)", completeFailure.message)
      assertSame(terminalError, completeFailure.cause)

      val expectingItem = testScheduler.virtualAsync { turbine.awaitItem() }
      advanceUntilIdle()
      val itemFailure = expectingItem.getCompletionExceptionOrNull()
      assertNotNull(itemFailure)
      assertEquals("Expected item but found Error(CustomThrowable)", itemFailure.message)
      assertSame(terminalError, itemFailure.cause)

      // ...and awaitError keeps yielding the exact same throwable instance on every read.
      repeat(replayCount) { readIndex ->
        val errorProbe = testScheduler.virtualAsync { turbine.awaitError() }
        advanceUntilIdle()
        assertSame(
          terminalError,
          errorProbe.getCompleted(),
          "read $readIndex: terminal error must be sticky",
        )
      }
    }
  }

  // endregion

  // region Nested turbines and scope-exit aggregation

  @Test
  fun nestedTurbineScopeReportsInnerUnconsumedExceptionOnOuterFailure() = runTest {
    val innerError = CustomThrowable("inner failure")

    val failure =
      assertFailsWith<AssertionError> {
        turbineScope {
          flow<Nothing> { throw innerError }.testIn(this, name = "nested-error")
          advanceUntilIdle()
          // Fail the outer validation for an unrelated reason; the inner unconsumed terminal
          // exception must still be attached with a diagnostic stack trace.
          throw AssertionError("outer validation failure")
        }
      }

    assertEquals("outer validation failure", failure.cause?.message)
    val message = failure.message.orEmpty()
    assertTrue(
      message.startsWith(
        """
        |Unconsumed exception found for nested-error:
        |
        |Stack trace:
        """
          .trimMargin()
      ),
      "message was:\n$message",
    )
    assertTrue(message.contains("CustomThrowable: inner failure"), "message was:\n$message")
  }

  @Test
  fun scopeExitReportsOnlyFlowsWithUnconsumedExceptions() = runTest {
    val failingError = CustomThrowable("good flow died")

    val failure =
      assertFailsWith<AssertionError> {
        turbineScope {
          flow<Nothing> { throw failingError }
            .testIn(this, name = "failing")
            .also { advanceUntilIdle() }
          // A healthy, fully-consumed sibling contributes nothing to the exception report.
          flowOf("ok").testIn(this, name = "healthy").also { healthy ->
            advanceUntilIdle()
            assertEquals("ok", healthy.awaitItem())
            healthy.awaitComplete()
          }
          // Fail the outer validation for an unrelated reason to enter the catch aggregation
          // path; a registered flow's unconsumed exception must still be attached.
          throw AssertionError("unrelated outer failure")
        }
      }

    assertEquals("unrelated outer failure", failure.cause?.message)
    val message = failure.message.orEmpty()
    assertTrue(message.contains("Unconsumed exception found for failing"), "message was:\n$message")
    assertTrue(message.contains("CustomThrowable: good flow died"), "message was:\n$message")
    assertTrue(!message.contains("healthy"), "healthy sibling must not be reported: $message")
  }

  @Test
  fun nestedVirtualTimeoutIsDrivenBySchedulerAndCarriesTimeoutCause() = runTest {
    val mark = TimeSource.Monotonic.markNow()
    repeat(replayCount) {
      val awaiter = testScheduler.virtualAsync {
        withTurbineTimeout(1.hours) { Turbine<Unit>().awaitItem() }
      }
      advanceUntilIdle()

      val failure = awaiter.getCompletionExceptionOrNull()
      assertNotNull(failure)
      assertTrue(failure is AssertionError)
      assertEquals("No value produced in 1h", failure.message)
      assertTrue(failure.cause is TimeoutCancellationException)
    }
    assertTrue(
      mark.elapsedNow() < 2.seconds,
      "nested virtual timeouts consumed real time: ${mark.elapsedNow()}",
    )
  }

  // endregion
}
