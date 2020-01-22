package executor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loadimpact/k6/lib/types"
	"github.com/loadimpact/k6/stats"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	null "gopkg.in/guregu/null.v3"
)

func getTestVariableArrivalRateConfig() VariableArrivalRateConfig {
	return VariableArrivalRateConfig{
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
}

func TestVariableArrivalRateRunNotEnoughAllocatedVUsWarn(t *testing.T) {
	t.Parallel()
	var ctx, cancel, executor, logHook = setupExecutor(
		t, getTestVariableArrivalRateConfig(),
		simpleRunner(func(ctx context.Context) error {
			time.Sleep(time.Second)
			return nil
		}),
	)
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
	var ctx, cancel, executor, logHook = setupExecutor(
		t, getTestVariableArrivalRateConfig(),
		simpleRunner(func(ctx context.Context) error {
			atomic.AddInt64(&count, 1)
			return nil
		}),
	)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// check that we got around the amount of VU iterations as we would expect
		var currentCount int64

		time.Sleep(time.Second)
		currentCount = atomic.SwapInt64(&count, 0)
		assert.InDelta(t, 10, currentCount, 1)

		time.Sleep(time.Second)
		currentCount = atomic.SwapInt64(&count, 0)
		assert.InDelta(t, 30, currentCount, 2)

		time.Sleep(time.Second)
		currentCount = atomic.SwapInt64(&count, 0)
		assert.InDelta(t, 50, currentCount, 2)
	}()
	var engineOut = make(chan stats.SampleContainer, 1000)
	err := executor.Run(ctx, engineOut)
	wg.Wait()
	require.NoError(t, err)
	require.Empty(t, logHook.Drain())
}

func TestVariableArrivalRateRunCorrectRateWithSlowRate(t *testing.T) {
	t.Parallel()
	var count int64
	var now = time.Now()
	var expectedTimes = []time.Duration{
		time.Millisecond * 3464, time.Millisecond * 4898, time.Second * 6}
	var ctx, cancel, executor, logHook = setupExecutor(
		t, VariableArrivalRateConfig{
			TimeUnit: types.NullDurationFrom(time.Second),
			Stages: []Stage{
				{
					Duration: types.NullDurationFrom(time.Second * 6),
					Target:   null.IntFrom(1),
				},
				{
					Duration: types.NullDurationFrom(time.Second * 0),
					Target:   null.IntFrom(0),
				},
				{
					Duration: types.NullDurationFrom(time.Second * 1),
					Target:   null.IntFrom(0),
				},
			},
			PreAllocatedVUs: null.IntFrom(10),
			MaxVUs:          null.IntFrom(20),
		},
		simpleRunner(func(ctx context.Context) error {
			current := atomic.AddInt64(&count, 1)
			if !assert.True(t, int(current) <= len(expectedTimes)) {
				return nil
			}
			expectedTime := expectedTimes[current-1]
			assert.WithinDuration(t,
				now.Add(expectedTime),
				time.Now(),
				time.Millisecond*100,
				"%d expectedTime %s", current, expectedTime,
			)
			return nil
		}),
	)
	defer cancel()
	var engineOut = make(chan stats.SampleContainer, 1000)
	err := executor.Run(ctx, engineOut)
	require.NoError(t, err)
	require.Equal(t, int64(len(expectedTimes)), count)
	require.Empty(t, logHook.Drain())
}

func TestVariableArrivalRateCal(t *testing.T) {
	t.Parallel()

	var expectedTimes = []time.Duration{
		time.Millisecond * 3162, time.Millisecond * 4472, time.Millisecond * 5527, time.Millisecond * 6837, time.Second * 10}
	var config = VariableArrivalRateConfig{
		TimeUnit:  types.NullDurationFrom(time.Second),
		StartRate: null.IntFrom(0),
		Stages: []Stage{
			{
				Duration: types.NullDurationFrom(time.Second * 5),
				Target:   null.IntFrom(1),
			},
			{
				Duration: types.NullDurationFrom(time.Second * 5),
				Target:   null.IntFrom(0),
			},
		},
	}

	var ch = make(chan time.Duration, 20)
	config.cal(ch)
	var changes = make([]time.Duration, 0, len(expectedTimes))
	for c := range ch {
		changes = append(changes, c)
	}
	assert.Equal(t, len(expectedTimes), len(changes))
	for i, expectedTime := range expectedTimes {
		change := changes[i]
		assert.InEpsilon(t, expectedTime, change, 0.001, "%s %s", expectedTime, change)
	}
}

func TestVariableArrivalRateCal2(t *testing.T) {
	t.Parallel()

	var expectedTimes = []time.Duration{
		time.Millisecond * 3162, time.Millisecond * 4472, time.Millisecond * 5500}
	var config = VariableArrivalRateConfig{
		TimeUnit:  types.NullDurationFrom(time.Second),
		StartRate: null.IntFrom(0),
		Stages: []Stage{
			{
				Duration: types.NullDurationFrom(time.Second * 5),
				Target:   null.IntFrom(1),
			},
			{
				Duration: types.NullDurationFrom(time.Second * 1),
				Target:   null.IntFrom(1),
			},
		},
	}

	var ch = make(chan time.Duration, 20)
	config.cal(ch)
	var changes = make([]time.Duration, 0, len(expectedTimes))
	for c := range ch {
		changes = append(changes, c)
	}
	assert.Equal(t, len(expectedTimes), len(changes))
	for i, expectedTime := range expectedTimes {
		change := changes[i]
		assert.InEpsilon(t, expectedTime, change, 0.001, "%s %s", expectedTime, change)
	}
}

func BenchmarkCal(b *testing.B) {
	for _, t := range []time.Duration{
		time.Second, time.Minute,
	} {
		t := t
		b.Run(t.String(), func(b *testing.B) {
			var config = VariableArrivalRateConfig{
				TimeUnit:  types.NullDurationFrom(time.Second),
				StartRate: null.IntFrom(500000),
				Stages: []Stage{
					{
						Duration: types.NullDurationFrom(t),
						Target:   null.IntFrom(499999),
					},
					{
						Duration: types.NullDurationFrom(t),
						Target:   null.IntFrom(500000),
					},
				},
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					var ch = make(chan time.Duration, 20)
					go config.cal(ch)
					for c := range ch {
						_ = c
					}
				}
			})
		})
	}
}

func BenchmarkCalRat(b *testing.B) {
	var config = VariableArrivalRateConfig{
		TimeUnit:  types.NullDurationFrom(time.Second),
		StartRate: null.IntFrom(0),
		Stages: []Stage{
			{
				Duration: types.NullDurationFrom(30 * time.Second),
				Target:   null.IntFrom(200),
			},
			{
				Duration: types.NullDurationFrom(1 * time.Minute),
				Target:   null.IntFrom(200),
			},
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var ch = make(chan time.Duration, 20)
			go config.calRat(ch)
			for c := range ch {
				_ = c
			}
		}
	})
}
