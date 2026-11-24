# xdag 行为对照

上游：[`9533372`](https://github.com/xmapst/xdag/tree/95333724258112c354f3ea618d72c58ba51bb42a)。

```sh
go test -race -count=1 -json ./...  # 在上述 Go checkout 中执行
cargo test --offline --all-targets # 在 Rust 仓库根目录执行
```

Go：140 个顶层测试、6 个示例、86 个表驱动子项全部通过（无失败、无跳过）。

表中 Rust 一栏列出覆盖同一行为的断言，**不表示复刻了整条 Go 用例或该用例的所有子项**。`—` 表示本次没有找到等价的 Rust 断言，不代表失败或一致。组合场景仅覆盖部分断言时也不能当作完整对测。示例断言需运行对应二进制，`cargo test` 编译示例二进制并不会执行它们的 `main`。

## 对照发现

- 退避：Go 把指数退避值和抖动后的值分别截断到整数纳秒。Rust 已改用同样的两次截断，并新增小数纳秒回归断言。
- 强制取消：Go 宽限期耗尽后给被放弃任务记录整场取消哨兵，不附加调用者传入的 cause；仍存活的任务上下文保留原始 cause。Rust 已补齐，并新增回归断言。
- 上述两处差异来自 Go 源码对照，新增断言在 Rust 侧。额外的 Go overlay 探针未获执行权限；它不计入对测结果。
- 未验证所有并发边界。源码中还有一个值得单独对测的分支：最后一次重试在并发闸门上被取消时，Go 可能沿用上一次业务错误并走重试耗尽分支；Rust 从并发等待直接返回中断并保留上次错误。该时序未做跨语言复现。
- Go 的 `errors.Is` / `errors.As`、panic 值和栈，以及 `runtime.Goexit` 不具有直接等价的 Rust 表示；Rust 断言检查状态和类型化错误，不比较语言特有的错误包装文本或堆栈。

## `concurrency_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestMaxConcurrencyIsRespected` | 通过 | `observe` 示例：并发限制、进度和观察者 |
| `TestUnlimitedConcurrencyByDefault` | 通过 | — |
| `TestSuspendedTaskDoesNotHoldConcurrencySlot` | 通过 | — |
| `TestBackoffDoesNotHoldConcurrencySlot` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestSkippedTasksDoNotConsumeSlots` | 通过 | — |
| `TestCancelWhileWaitingForSlot` | 通过 | — |
| `TestPanicErrorIsStructured` | 通过 | `retry_hooks` 示例：结果摘要、业务错误和 panic |
| `TestPanicErrorZeroAttemptWording` | 通过 | — |
| `TestNoGoroutineLeak` | 通过（含 8 子项） | 语言差异：Go 运行时专用；Rust 尚未做对应的 Tokio 任务泄漏检查 |

## `control_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestCancelYieldsCanceledStateAndOneError` | 通过 | `src/executor.rs` 取消去重单测 / `cancel_grace` 示例 |
| `TestExecuteWithAlreadyCanceledContext` | 通过 | — |
| `TestDeadlineExceededYieldsCanceled` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestCanceledUpstreamPropagatesAsCanceled` | 通过 | `tests/labeled_edges.rs`：取消优先级 |
| `TestCancelTaskCancelsOnlyThatTaskAndCascades` | 通过 | `task_control` 示例：独立挂起、单任务取消和级联错误 |
| `TestSuspendTaskBlocksBetweenRetries` | 通过 | — |
| `TestSuspendResumeCanCycleRepeatedly` | 通过 | — |
| `TestResumeIsIdempotentAndCancelUnblocksSuspended` | 通过 | — |
| `TestActionsOnUnknownOrDoneTask` | 通过 | — |
| `TestCancelBeforeExecute` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestSuspendBeforeExecute` | 通过 | — |
| `TestSuspendedTaskDestinedToSkipDoesNotHang` | 通过 | — |
| `TestCancelLetsInFlightFinishAndBlocksNewOnes` | 通过 | — |
| `TestCancelBeforeExecuteRunsNothing` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestCancelDoesNotMaskSkipped` | 通过 | — |
| `TestCancelDoesNotInterruptRunningAttempt` | 通过 | — |
| `TestCancelCutsShortBackoffWait` | 通过 | — |
| `TestCancelWakesSuspendedTask` | 通过 | — |
| `TestCancelWakesTasksQueuedOnConcurrencyGate` | 通过 | — |
| `TestCancelWaitsForTaskBody` | 通过 | `tests/control_parity.rs` + `cancel_grace` 示例：取消宽限期 |
| `TestRepeatabilityFollowsFromTerminalRule` | 通过（含 3 子项） | — |
| `TestCancelTaskCarriesCallerCause` | 通过 | `task_control` 示例：独立挂起、单任务取消和级联错误 |
| `TestCancelCarriesCallerCause` | 通过 | `tests/control_parity.rs` + `cancel_grace` 示例：取消宽限期 |
| `TestNilCauseKeepsSentinelOnly` | 通过 | — |
| `TestAbandonDoesNotReportSettledTasks` | 通过 | — |

## `edge_internal_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestEdgeSignalTable` | 通过（含 21 子项） | — |
| `TestDecideTable` | 通过（含 22 子项） | — |
| `TestDefaultEdgesEquivalentToAllDepsSucceeded` | 通过 | — |
| `TestEmitSetNormalizes` | 通过（含 5 子项） | `tests/labeled_edges.rs`：空标签与平行边 |

## `edge_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestAfterBuildsSuccessEdges` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestEmptyOnBehavesAsSuccessEdge` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestNewDoesNotMutateCallerDeps` | 通过 | — |

## `example_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `Example` | 通过 | — |
| `Example_states` | 通过 | — |
| `Example_businessCondition` | 通过 | — |
| `Example_subgraph` | 通过 | — |
| `ExampleScheduler_switch` | 通过 | `tests/labeled_edges.rs`：分支与汇合 |
| `ExampleScheduler_failureHandler` | 通过 | `tests/labeled_edges.rs`：失败边与图标签 |

## `execute_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestLinearChainExecution` | 通过 | — |
| `TestDiamondExecution` | 通过 | — |
| `TestDependencyOutputsVisibleAsInputs` | 通过 | `tests/labeled_edges.rs`：分支与汇合 |
| `TestManyRootsNoRace` | 通过 | — |
| `TestFailureDoesNotStallDownstream` | 通过 | `validation` 示例：失败子树、校验和部分输出 |
| `TestFailureIsScopedToItsOwnSubtree` | 通过 | `validation` 示例：失败子树、校验和部分输出 |
| `TestResultsOnlyContainSuccessfulTasks` | 通过 | `validation` 示例：失败子树、校验和部分输出 |
| `TestExecuteTwiceIsRejected` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestStateOfUnknownTaskIsPending` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestPrintGraphDoesNotPanic` | 通过 | — |
| `TestUnstableDependenciesAreSnapshotAtNew` | 通过 | `tests/labeled_edges.rs`：拓扑校验与冻结快照 |

## `failure_edge_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestFailureEdgeRunsHandlerWithUpstreamError` | 通过 | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestFailureEdgeNotFiredOnSuccess` | 通过 | — |
| `TestFailureEdgeFiresOnlyAfterRetriesExhausted` | 通过 | — |
| `TestFailureEdgeIgnoredForCanceledAndSkipped` | 通过（含 2 子项） | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestFailureHandlerOwnFailureIsAggregated` | 通过 | — |

## `graph_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestUnknownDependencyReturnsError` | 通过 | `tests/labeled_edges.rs`：拓扑校验与冻结快照 |
| `TestCycleDetection` | 通过 | `tests/labeled_edges.rs`：拓扑校验与冻结快照 |
| `TestSelfDependencyIsCycle` | 通过 | — |
| `TestValidateAcceptsWellFormedGraph` | 通过 | — |
| `TestValidateRejectsNilTask` | 通过 | 语言差异：`BoxTask`（`Arc<dyn Task>`）不可为 `nil` |
| `TestWriteGraphDeduplicatesJoinPoints` | 通过 | — |
| `TestWriteGraphDoesNotBlowUpOnStackedDiamonds` | 通过 | — |
| `TestWriteGraphUsesFrozenDeps` | 通过 | `tests/labeled_edges.rs`：拓扑校验与冻结快照 |
| `TestWriteGraphIsDeterministic` | 通过 | — |
| `TestWriteGraphLabelsNonSuccessEdges` | 通过 | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestWriteGraphLabelBeforeAnyOfMarker` | 通过 | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestWriteGraphMultipleEdgesToSameChildExpandOnce` | 通过 | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestWriteGraphJoinPointRepeatKeepsLabelsBeforeEllipsis` | 通过 | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestWriteGraphLabelsAreDeterministic` | 通过 | — |
| `TestWriteGraphPropagatesWriteError` | 通过 | — |

## `iface_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestISchedulerMirrorsScheduler` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |

## `observer_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestObserverFiresOncePerTask` | 通过 | `observe` 示例：并发限制、进度和观察者 |
| `TestObserverCanQueryWithoutDeadlock` | 通过 | — |
| `TestObserverPanicIsContained` | 通过 | `observe` 示例：并发限制、进度和观察者 |
| `TestNoObserverIsHarmless` | 通过 | — |
| `TestObserverIsCalledConcurrently` | 通过 | — |

## `progress_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestProgressCounts` | 通过 | — |
| `TestProgressBeforeExecute` | 通过 | — |
| `TestProgressDuringExecution` | 通过 | `observe` 示例：并发限制、进度和观察者 |
| `TestProgressEmptyGraph` | 通过 | — |
| `TestProgressString` | 通过 | — |
| `TestQueriesAreSafeUnderConcurrentExecution` | 通过 | — |
| `TestObserverPanicIsStructured` | 通过 | `observe` 示例：并发限制、进度和观察者 |
| `TestObserverPanicDoesNotEvictTaskErrors` | 通过 | — |
| `TestCancelWaitsForDrain` | 通过 | `tests/control_parity.rs` + `cancel_grace` 示例：取消宽限期 |
| `TestCancelGivesUpWaitingOnContext` | 通过 | `tests/control_parity.rs` + `cancel_grace` 示例：取消宽限期 |
| `TestCancelBeforeExecuteReturnsImmediately` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestCancelUnblocksOnPanicUnwind` | 通过 | — |

## `retry_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestRetryEventuallySucceeds` | 通过 | `retry_hooks` 示例：结果摘要、业务错误和 panic |
| `TestRetryExhaustsAttempts` | 通过 | — |
| `TestZeroMaxAttemptsRunsOnce` | 通过（含 2 子项） | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestInfiniteAttemptsIsExplicitAndCancelable` | 通过 | — |
| `TestBackoffDoesNotOverflowPastMaxInterval` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestBackoffUnderflowDoesNotJumpToMaxInterval` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestBackoffWithoutJitterIsDeterministic` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestJitterOnlyShortensNeverLengthens` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestJitterOutOfRangeIsClamped` | 通过（含 5 子项） | `src/retry.rs` 单测：重试次数、退避、并发槽 |
| `TestJitterAppliesAtMaxInterval` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |

## `robustness_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestTaskPanicBecomesFailure` | 通过 | `retry_hooks` 示例：结果摘要、业务错误和 panic |
| `TestCallbackPanicBecomesFailure` | 通过 | — |
| `TestPanicIsRetriedLikeAnyOtherFailure` | 通过 | — |
| `TestInputIsIsolatedPerDownstream` | 通过 | 语言差异：Go 可修改输入 map；Rust 提供只读 `&Input`，每个下游持有独立 map |
| `TestGoexitIsReportedAndCascades` | 通过 | 语言差异：Go 运行时专用；Rust 尚未做对应的 Tokio 任务泄漏检查 |
| `TestNonRetryableErrorStopsRetrying` | 通过 | `retry_hooks` 示例：结果摘要、业务错误和 panic |
| `TestResultsReportStateErrAndAttempts` | 通过 | `retry_hooks` 示例：结果摘要、业务错误和 panic |
| `TestResultsBeforeExecute` | 通过 | — |
| `TestPhaseLifecycleAndPrecedence` | 通过（含 7 子项） | — |
| `TestPhaseStringAndDone` | 通过 | — |
| `TestSkippedImpliesFailedInvariant` | 通过（含 4 子项） | — |
| `TestGoexitOnLaterAttemptIsStillCaught` | 通过 | 语言差异：Go 运行时专用；Rust 尚未做对应的 Tokio 任务泄漏检查 |
| `TestSecondExecuteDoesNotResetPhase` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestAttemptsNonZeroWhenCanceledMidFlight` | 通过 | `tests/control_parity.rs` + `cancel_grace` 示例：取消宽限期 |
| `TestAttemptsZeroWhenPreCanceled` | 通过 | `tests/control_parity.rs`：启动前状态、取消、超时以及二次执行 |
| `TestCascadedCancelHasNilErr` | 通过 | — |
| `TestPhaseDoesNotLieOnPanicUnwind` | 通过 | — |
| `TestWaitInterruptionPreservesLastError` | 通过（含 3 子项） | — |
| `TestObserverPanicIsNotTaskPanic` | 通过 | `observe` 示例：并发限制、进度和观察者 |
| `TestPanicCarryingNonRetryableStopsRetrying` | 通过 | — |
| `TestMultiplierNaNAndInfAreDefaulted` | 通过 | `src/retry.rs` 单测：重试次数、退避、并发槽 |

## `run_suspend_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestSuspendHoldsWholeRun` | 通过 | — |
| `TestRunAndTaskSuspendAreIndependent` | 通过 | `task_control` 示例：独立挂起、单任务取消和级联错误 |
| `TestRunSuspendDoesNotUnstickTaskSuspend` | 通过 | `task_control` 示例：独立挂起、单任务取消和级联错误 |
| `TestTaskNameMustMatchKey` | 通过 | `validation` 示例：失败子树、校验和部分输出 |

## `switch_test.go`

| Go 用例 | Go | Rust 证据或差异 |
| --- | --- | --- |
| `TestSwitchEmitsChosenBranchOnly` | 通过 | `tests/labeled_edges.rs`：分支与汇合 |
| `TestSwitchMultiEmit` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestAnyOfJoinAfterSwitchRunsOnce` | 通过 | `tests/labeled_edges.rs`：分支与汇合 |
| `TestAllOfJoinAfterSwitchIsSkipped` | 通过 | `tests/labeled_edges.rs`：分支与汇合 |
| `TestEmitWithoutMatchingEdgeSkipsDownstream` | 通过 | `tests/labeled_edges.rs`：分支与汇合 |
| `TestEmitOnFailureIsRejected` | 通过 | `tests/labeled_edges.rs`：拒绝保留标签 |
| `TestEmitOnFailureRejectedAnywhereInEmit` | 通过 | `tests/labeled_edges.rs`：拒绝保留标签 |
| `TestEmitEmptyStringIsOnSuccess` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestEmitEmptyStringAlongsideCustomLabel` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestSameDepSuccessAndFailureEdgesIntoAnyOf` | 通过（含 2 子项） | `tests/labeled_edges.rs`：失败边与图标签 |
| `TestAnyOfRootStillRuns` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestCanceledBeatsActiveInAnyOf` | 通过 | `tests/labeled_edges.rs`：取消优先级 |
| `TestObserverFiresOncePerTaskWithSwitch` | 通过 | — |
| `TestDuplicateEdgesFromSameUpstreamDoNotHang` | 通过 | `tests/labeled_edges.rs`：空标签与平行边 |
| `TestSameDepSuccessAndFailureEdgesIntoAllOfIsAlwaysSkipped` | 通过（含 2 子项） | — |
