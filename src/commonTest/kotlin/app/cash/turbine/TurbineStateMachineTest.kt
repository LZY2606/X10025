package app.cash.turbine

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletionHandlerException
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext

/**
 * State-machine coverage for the interaction of timeout, cancellation, and terminal events.
 *
 * A [ReceiveTurbine] moves through these states:
 * ```
 * COLLECTING --item--> COLLECTING
 * COLLECTING --complete--> COMPLETED
 * COLLECTING --error--> FAILED
 * COLLECTING --cancel()--> CANCELLED (terminal events ignored, items still reportable)
 * any --terminal event consumed--> IGNORE-REMAINING (scope exit is silent)
 * any --scope exit with unconsumed events--> AssertionError
 * ```
 *
 * Each test documents the design assumption it is able to falsify. No test waits on real wall-clock
 * time: virtual time is advanced explicitly, and the single timeout-expiry case uses a fixed 10ms
 * timeout which always expires (it can never race an emission).
 */
class TurbineStateMachineTest {
  @Test
  fun virtualTimeGatesEmissionsWithoutRealWaiting() = runTest {
    // Falsifies: "observing delayed emissions requires wall-clock waiting".
    flow {
      delay(1_000)
      emit("one")
      delay(1_000)
      emit("two")
    }
      .test {
        expectNoEvents()
        advanceTimeBy(999)
        runCurrent()
        expectNoEvents()
        advanceTimeBy(1)
        runCurrent()
        assertEquals("one", awaitItem())
        expectNoEvents()
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals("two", awaitItem())
        awaitComplete()
      }
  }

  @Test
  fun virtualTimeBeyondTimeoutDoesNotTripWallclockTimeout() = runTest {
    // Falsifies: "the turbine timeout is enforced in virtual time".
    // Regression case for the most dangerous counterexample of Flow testing with virtual time:
    // if the timeout were virtual, advancing the scheduler past it would fail every delayed
    // emission (and runTest's final advanceUntilIdle would trip any pending timeout).
    flow {
      delay(10.seconds)
      emit("late")
    }
      .test {
        advanceTimeBy(10.seconds)
        runCurrent()
        assertEquals("late", awaitItem())
        awaitComplete()
      }
  }

  @Test
  fun timeoutFailureLeavesTurbineReadable() = runTest {
    // Falsifies: "a timeout moves the turbine into a terminal failed state".
    // The fixed 10ms timeout always expires because nothing is ever emitted before it; this is
    // a deterministic timeout, not a sleep used for synchronization.
    val source = MutableSharedFlow<String>(extraBufferCapacity = 1)
    source.test(timeout = 10.milliseconds) {
      val timedOut = assertFailsWith<AssertionError> { awaitItem() }
      assertEquals("No value produced in 10ms", timedOut.message)
      assertTrue(timedOut.cause is CancellationException)

      assertTrue(source.tryEmit("after"))
      assertEquals("after", awaitItem())
      cancelAndIgnoreRemainingEvents()
    }
  }

  @Test
  fun parentCancellationReportsUnconsumedComplete() = runTest {
    // Falsifies: "terminal events are silently dropped when the parent scope is cancelled".
    turbineScope {
      val exceptionHandler = RecordingExceptionHandler()
      launch(start = UNDISPATCHED) {
          withContext(exceptionHandler) { emptyFlow<Nothing>().testIn(this) }
        }
        .cancel()
      val exception = exceptionHandler.exceptions.removeFirst()
      assertTrue(exception is CompletionHandlerException)
      val cause = exception.cause
      assertTrue(cause is AssertionError)
      assertEquals(
        """
        |Unconsumed events found:
        | - Complete
        """
          .trimMargin(),
        cause.message,
      )
      assertNull(cause.cause)
    }
  }

  @Test
  fun parentCancellationAfterFullConsumptionRecordsNoFailure() = runTest {
    // Falsifies: "any external cancellation of the parent scope fails the test".
    turbineScope {
      val exceptionHandler = RecordingExceptionHandler()
      launch(start = UNDISPATCHED) {
          withContext(exceptionHandler) {
            val turbine = flowOf("a").testIn(this)
            assertEquals("a", turbine.awaitItem())
            turbine.awaitComplete()
          }
        }
        .cancel()
      assertTrue(exceptionHandler.exceptions.isEmpty())
    }
  }

  @Test
  fun repeatedCancelIsIdempotent() = runTest {
    // Falsifies: "cancel() is one-shot; a second cancel re-arms or corrupts the state machine".
    // A standalone turbine has no collect job, so cancel() itself produces the terminal
    // event (Complete); ignoring it requires cancel() to arm ignore-terminal-events.
    val turbine = Turbine<String>()
    turbine.add("consumed")
    assertEquals("consumed", turbine.awaitItem())
    turbine.cancel()
    turbine.cancel()
    turbine.cancel()
    // No items pending and the terminal event is ignored: this must not throw.
    turbine.ensureAllEventsConsumed()
  }

  @Test
  fun cancelKeepsBufferedEventsObservable() = runTest {
    // Falsifies: "cancel() discards buffered events" (the design closes, not cancels, the
    // underlying channel so that already-produced events remain observable).
    flowOf("a", "b").test {
      cancel()
      assertEquals(
        listOf<Event<String>>(Event.Item("a"), Event.Item("b"), Event.Complete),
        cancelAndConsumeRemainingEvents(),
      )
    }
  }

  @Test
  fun completeIsTerminalAndRereadable() = runTest {
    // Falsifies: "a terminal event can only be observed once" and "re-reading a terminal event
    // leaves the turbine out of sync with the unconsumed-events check".
    emptyFlow<Unit>().test {
      awaitComplete()
      val misread = assertFailsWith<AssertionError> { awaitItem() }
      assertEquals("Expected item but found Complete", misread.message)
      assertNull(misread.cause)
      // Scope exit must stay silent: consuming Complete armed ignore-remaining.
    }
  }

  @Test
  fun errorIsTerminalAndRereadableWithCauseIntact() = runTest {
    // Falsifies: "the upstream failure is lost once the error event has been consumed".
    val boom = CustomThrowable("terminal-boom")
    flow<Unit> { throw boom }
      .test {
        assertSame(boom, awaitError())
        val misread = assertFailsWith<AssertionError> { awaitItem() }
        assertEquals("Expected item but found Error(CustomThrowable)", misread.message)
        assertSame(boom, misread.cause)
      }
  }

  @Test
  fun nestedTurbinesOperateIndependently() = runTest {
    // Falsifies: "nested turbines share consumption state".
    flowOf("outer-1", "outer-2").test {
      flowOf("inner-1").test {
        assertEquals("inner-1", awaitItem())
        awaitComplete()
      }
      assertEquals("outer-1", awaitItem())
      assertEquals("outer-2", awaitItem())
      awaitComplete()
    }
  }

  @Test
  fun nestedFailureIsAttributedToInnerTurbine() = runTest {
    // Falsifies: "a nested failure is attributed to the outer turbine, or swallowed by it".
    val actual =
      assertFailsWith<AssertionError> {
        flowOf("outer").test(name = "outer") {
          assertEquals("outer", awaitItem())
          awaitComplete()
          flowOf("inner").test(name = "inner") {
            // Consume nothing: the inner turbine alone must be reported.
          }
        }
      }
    assertEquals(
      """
      |Unconsumed events found for inner:
      | - Item(inner)
      | - Complete
      """
        .trimMargin(),
      actual.message,
    )
    assertNull(actual.cause)
  }

  @Test
  fun unconsumedTerminalEventReportMatrix() = runTest {
    // Boundary matrix over the state machine:
    //   end-of-collection: OPEN-COLD (suspending) x STANDALONE x COMPLETE x ERROR
    //   pending items:     0, 1, 3
    //   consumed items:    0..pending
    // The two cancel rows exist because the terminal event after cancel() depends on the
    // upstream: a suspending cold flow is closed with a CancellationException (stripped
    // from the report), while a standalone turbine lets cancel()'s own close win
    // (Complete, ignored via ignore-terminal-events).
    // Falsifies: "cancel() treats terminal events the same regardless of whether upstream
    // already terminated" and "the report drops pending items or the failure cause".
    val boom = CustomThrowable("matrix-boom")

    fun expectedMessage(unconsumed: List<String>) = buildString {
      append("Unconsumed events found:")
      for (event in unconsumed) {
        append("\n - ")
        append(event)
      }
    }

    for (itemCount in listOf(0, 1, 3)) {
      for (consumed in 0..itemCount) {
        val items = (0 until itemCount).map { "item-$it" }
        val remainingItems = items.drop(consumed).map { "Item($it)" }
        val case = "itemCount=$itemCount consumed=$consumed"

        // OPEN-COLD: upstream still suspended at cancel(); its cancellation closes the
        // channel with a CancellationException, which is stripped from the report.
        val openColdFlow =
          flow<String> {
            for (item in items) emit(item)
            awaitCancellation()
          }
        if (remainingItems.isEmpty()) {
          openColdFlow.test {
            repeat(consumed) { awaitItem() }
            cancel()
          }
        } else {
          val actual =
            assertFailsWith<AssertionError> {
              openColdFlow.test {
                repeat(consumed) { awaitItem() }
                cancel()
              }
            }
          assertEquals(expectedMessage(remainingItems), actual.message, "OPEN-COLD $case")
          assertNull(actual.cause, "OPEN-COLD $case")
        }

        // STANDALONE: no collect job, so cancel() closes the channel normally and the
        // terminal event is Complete, ignored via ignore-terminal-events. (For a collected
        // hot flow the winning terminal event is dispatcher-dependent, so the standalone
        // turbine is the deterministic home for this row.)
        val standalone = Turbine<String>()
        for (item in items) standalone.add(item)
        repeat(consumed) { standalone.awaitItem() }
        standalone.cancel()
        if (remainingItems.isEmpty()) {
          standalone.ensureAllEventsConsumed()
        } else {
          val actual = assertFailsWith<AssertionError> { standalone.ensureAllEventsConsumed() }
          assertEquals(expectedMessage(remainingItems), actual.message, "STANDALONE $case")
          assertNull(actual.cause, "STANDALONE $case")
        }

        // COMPLETE: upstream already closed; the terminal event is reportable.
        val completeFlow =
          flow<String> {
            for (item in items) emit(item)
          }
        val completeActual =
          assertFailsWith<AssertionError> {
            completeFlow.test {
              repeat(consumed) { awaitItem() }
              cancel()
            }
          }
        assertEquals(
          expectedMessage(remainingItems + "Complete"),
          completeActual.message,
          "COMPLETE $case",
        )
        assertNull(completeActual.cause, "COMPLETE $case")

        // ERROR: upstream already failed; the error is reportable and becomes the cause.
        val errorFlow =
          flow<String> {
            for (item in items) emit(item)
            throw boom
          }
        val errorActual =
          assertFailsWith<AssertionError> {
            errorFlow.test {
              repeat(consumed) { awaitItem() }
              cancel()
            }
          }
        assertEquals(
          expectedMessage(remainingItems + "Error(CustomThrowable)"),
          errorActual.message,
          "ERROR $case",
        )
        assertSame(boom, errorActual.cause, "ERROR $case")
      }
    }
  }

  @Test
  fun generatedTracesAreReconstructedByConsumingCancel() = runTest {
    // Generative property over replayable traces (fixed seed, no randomness across runs):
    // for any trace of items followed by a terminal event, and any consumed prefix,
    // cancelAndConsumeRemainingEvents() returns exactly the remaining suffix ending in the
    // terminal event, and consumed ++ remaining reconstructs the full trace.
    // Falsifies: "events are reordered, duplicated, or dropped at the cancel boundary".
    val random = Random(200619)
    repeat(25) { iteration ->
      val itemCount = random.nextInt(0, 5)
      val items = List(itemCount) { "trace-$iteration-$it" }
      val failure = if (random.nextBoolean()) CustomThrowable("trace-$iteration-boom") else null
      val consumeCount = random.nextInt(0, itemCount + 1)

      val trace = flow {
        for (item in items) emit(item)
        failure?.let { throw it }
      }

      trace.test {
        val consumed = mutableListOf<Event<String>>()
        repeat(consumeCount) { consumed += Event.Item(awaitItem()) }

        val remaining = cancelAndConsumeRemainingEvents()

        val terminal: Event<String> = failure?.let { Event.Error(it) } ?: Event.Complete
        val fullTrace = items.map { Event.Item(it) } + terminal
        assertEquals(
          fullTrace.drop(consumeCount),
          remaining,
          "iteration=$iteration items=$items failure=$failure consumeCount=$consumeCount",
        )
        assertTrue(remaining.last().isTerminal, "iteration=$iteration")
        assertEquals(fullTrace, consumed + remaining, "iteration=$iteration")
      }
    }
  }
}
