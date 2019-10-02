package executor

import (
	"context"
	"io/ioutil"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loadimpact/k6/lib"
	"github.com/loadimpact/k6/lib/testutils"
	"github.com/loadimpact/k6/lib/types"
	"github.com/loadimpact/k6/stats"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	null "gopkg.in/guregu/null.v3"
)

func TestGetPlannedRateChanges0DurationStage(t *testing.T) {
	t.Parallel()
	var config = VariableArrivalRateConfig{
		TimeUnit:  types.NullDurationFrom(time.Second),
		StartRate: null.IntFrom(0),
		Stages: []Stage{
			{
				Duration: types.NullDurationFrom(0),
				Target:   null.IntFrom(50),
			},
			{
				Duration: types.NullDurationFrom(time.Minute),
				Target:   null.IntFrom(50),
			},
		},
	}
	var v *lib.ExecutionSegment
	changes := config.getPlannedRateChanges(v)
	require.Equal(t, 1, len(changes))
	require.Equal(t, time.Duration(0), changes[0].timeOffset)
	require.Equal(t, types.NullDurationFrom(time.Millisecond*20), changes[0].tickerPeriod)
}

func TestGetPlannedRateChanges(t *testing.T) {
	// TODO: Make multiple of those tests
	t.Parallel()
	var config = VariableArrivalRateConfig{
		TimeUnit:  types.NullDurationFrom(time.Second),
		StartRate: null.IntFrom(0),
		Stages: []Stage{
			{
				Duration: types.NullDurationFrom(2 * time.Minute),
				Target:   null.IntFrom(50),
			},
			{
				Duration: types.NullDurationFrom(time.Minute),
				Target:   null.IntFrom(50),
			},
			{
				Duration: types.NullDurationFrom(time.Minute),
				Target:   null.IntFrom(100),
			},
			{
				Duration: types.NullDurationFrom(0),
				Target:   null.IntFrom(200),
			},

			{
				Duration: types.NullDurationFrom(time.Second * 23),
				Target:   null.IntFrom(50),
			},
		},
	}

	var v *lib.ExecutionSegment
	changes := config.getPlannedRateChanges(v)
	for i, change := range changes {
		require.Equal(t, time.Duration(0),
			change.timeOffset%minIntervalBetweenRateAdjustments, "%d index", i)
		switch {
		case change.timeOffset <= time.Minute*2:
			var oneRat = new(big.Rat).Mul(
				big.NewRat(1000000000, 50),
				big.NewRat((2*time.Minute).Nanoseconds(), change.timeOffset.Nanoseconds()))
			var one = types.Duration(new(big.Int).Div(oneRat.Num(), oneRat.Denom()).Int64())
			var two = change.tickerPeriod.Duration
			require.Equal(t, one, two, "%d index %s %s", i, one, two)
		case change.timeOffset < time.Minute*4:
			var coef = big.NewRat(
				(change.timeOffset - time.Minute*3).Nanoseconds(),
				(time.Minute).Nanoseconds(),
			)
			var oneRat = new(big.Rat).Mul(big.NewRat(50, 1), coef)
			oneRat = oneRat.Add(oneRat, big.NewRat(50, 1))
			oneRat = new(big.Rat).Mul(big.NewRat(int64(time.Second), 1), new(big.Rat).Inv(oneRat))
			var one = types.Duration(new(big.Int).Div(oneRat.Num(), oneRat.Denom()).Int64())
			var two = change.tickerPeriod.Duration
			require.Equal(t, two, one, "%d index %s %s", i, one, two)
		case change.timeOffset == time.Minute*4:
			assert.Equal(t, change.tickerPeriod.Duration, types.Duration(time.Millisecond*5))
		default:
			var coef = big.NewRat(
				(change.timeOffset - time.Minute*4).Nanoseconds(),
				(time.Second * 23).Nanoseconds(),
			)
			var oneRat = new(big.Rat).Mul(big.NewRat(150, 1), coef)
			oneRat = new(big.Rat).Sub(big.NewRat(200, 1), oneRat)
			oneRat = new(big.Rat).Mul(big.NewRat(int64(time.Second), 1), new(big.Rat).Inv(oneRat))
			var one = types.Duration(new(big.Int).Div(oneRat.Num(), oneRat.Denom()).Int64())
			var two = change.tickerPeriod.Duration
			require.Equal(t, two, one, "%d index %s %s", i, one, two)
		}
	}
}

func initializeVUs(
	ctx context.Context, t testing.TB, logEntry *logrus.Entry, es *lib.ExecutionState, number int,
) {
	for i := 0; i < number; i++ {
		require.EqualValues(t, i, es.GetInitializedVUsCount())
		vu, err := es.InitializeNewVU(ctx, logEntry)
		require.NoError(t, err)
		require.EqualValues(t, i+1, es.GetInitializedVUsCount())
		es.ReturnVU(vu, false)
		require.EqualValues(t, 0, es.GetCurrentlyActiveVUsCount())
		require.EqualValues(t, i+1, es.GetInitializedVUsCount())
	}
}

func testVariableArrivalRateSetup(t *testing.T, vuFn func(context.Context, chan<- stats.SampleContainer) error) (
	context.Context, context.CancelFunc, lib.Executor, *testutils.SimpleLogrusHook) {
	ctx, cancel := context.WithCancel(context.Background())
	var config = VariableArrivalRateConfig{
		TimeUnit:  types.NullDurationFrom(time.Second),
		StartRate: null.IntFrom(10),
		Stages: []Stage{
			{
				Duration: types.NullDurationFrom(time.Second * 1),
				Target:   null.IntFrom(10),
			},
			{
				Duration: types.NullDurationFrom(time.Second * 1),
				Target:   null.IntFrom(50),
			},
			{
				Duration: types.NullDurationFrom(time.Second * 1),
				Target:   null.IntFrom(50),
			},
		},
		PreAllocatedVUs: null.IntFrom(10),
		MaxVUs:          null.IntFrom(20),
	}
	logHook := &testutils.SimpleLogrusHook{HookedLevels: []logrus.Level{logrus.WarnLevel}}
	testLog := logrus.New()
	testLog.AddHook(logHook)
	testLog.SetOutput(ioutil.Discard)
	logEntry := logrus.NewEntry(testLog)
	es := lib.NewExecutionState(lib.Options{}, 10, 50) //TODO: fix
	runner := lib.MiniRunner{
		Fn: vuFn,
	}

	es.SetInitVUFunc(func(_ context.Context, _ *logrus.Entry) (lib.VU, error) {
		return &lib.MiniRunnerVU{R: runner}, nil
	})

	initializeVUs(ctx, t, logEntry, es, 10)

	executor, err := config.NewExecutor(es, logEntry)
	require.NoError(t, err)
	err = executor.Init(ctx)
	require.NoError(t, err)
	return ctx, cancel, executor, logHook
}

func TestVariableArrivalRateRunNotEnoughAlloctedVUsWarn(t *testing.T) {
	t.Parallel()
	var ctx, cancel, executor, logHook = testVariableArrivalRateSetup(
		t, func(ctx context.Context, out chan<- stats.SampleContainer) error {
			time.Sleep(time.Second)
			return nil
		})
	defer cancel()
	var engineOut = make(chan stats.SampleContainer, 1000)
	err := executor.Run(ctx, engineOut)
	require.NoError(t, err)
	entries := logHook.Drain()
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		require.Equal(t,
			"Insufficient VUs, reached 20 active VUs and cannot allocate more",
			entry.Message)
		require.Equal(t, logrus.WarnLevel, entry.Level)
	}
}

func TestVariableArrivalRateRunCorrectRate(t *testing.T) {
	t.Parallel()
	var count int64
	var ctx, cancel, executor, logHook = testVariableArrivalRateSetup(
		t, func(ctx context.Context, out chan<- stats.SampleContainer) error {
			atomic.AddInt64(&count, 1)
			return nil
		})
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// check that we got around the amount of VU iterations as we would expect
		var currentCount int64

		time.Sleep(time.Second)
		currentCount = atomic.SwapInt64(&count, 0)
		require.InDelta(t, 10, currentCount, 1)

		time.Sleep(time.Second)
		currentCount = atomic.SwapInt64(&count, 0)
		// this is highly dependant on minIntervalBetweenRateAdjustments
		// TODO find out why this isn't 30 and fix it
		require.InDelta(t, 23, currentCount, 2)

		time.Sleep(time.Second)
		currentCount = atomic.SwapInt64(&count, 0)
		require.InDelta(t, 50, currentCount, 2)
	}()
	var engineOut = make(chan stats.SampleContainer, 1000)
	err := executor.Run(ctx, engineOut)
	wg.Wait()
	require.NoError(t, err)
	require.Empty(t, logHook.Drain())
}

func TestVariableArrivalRateCancel(t *testing.T) {
	t.Parallel()
	var ch = make(chan struct{})
	var errCh = make(chan error, 1)
	var weAreDoneCh = make(chan struct{})
	var ctx, cancel, executor, logHook = testVariableArrivalRateSetup(
		t, func(ctx context.Context, out chan<- stats.SampleContainer) error {
			select {
			case <-ch:
				<-ch
			default:
			}
			return nil
		})
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		var engineOut = make(chan stats.SampleContainer, 1000)
		errCh <- executor.Run(ctx, engineOut)
		close(weAreDoneCh)
	}()

	time.Sleep(time.Second)
	ch <- struct{}{}
	cancel()
	time.Sleep(time.Second)
	select {
	case <-weAreDoneCh:
		t.Fatal("Run raturned before all VU iterations were finished")
	default:
	}
	close(ch)
	<-weAreDoneCh
	wg.Wait()
	require.NoError(t, <-errCh)
	require.Empty(t, logHook.Drain())
}
