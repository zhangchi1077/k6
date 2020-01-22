/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2019 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package executor

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/loadimpact/k6/lib"
	"github.com/loadimpact/k6/lib/types"
	"github.com/loadimpact/k6/stats"
	"github.com/loadimpact/k6/ui/pb"
	"github.com/sirupsen/logrus"
	null "gopkg.in/guregu/null.v3"
)

const variableArrivalRateType = "variable-arrival-rate"

func init() {
	lib.RegisterExecutorConfigType(
		variableArrivalRateType,
		func(name string, rawJSON []byte) (lib.ExecutorConfig, error) {
			config := NewVariableArrivalRateConfig(name)
			err := lib.StrictJSONUnmarshal(rawJSON, &config)
			return config, err
		},
	)
}

// VariableArrivalRateConfig stores config for the variable arrival-rate executor
type VariableArrivalRateConfig struct {
	BaseConfig
	StartRate null.Int           `json:"startRate"`
	TimeUnit  types.NullDuration `json:"timeUnit"`
	Stages    []Stage            `json:"stages"`

	// Initialize `PreAllocatedVUs` number of VUs, and if more than that are needed,
	// they will be dynamically allocated, until `MaxVUs` is reached, which is an
	// absolutely hard limit on the number of VUs the executor will use
	PreAllocatedVUs null.Int `json:"preAllocatedVUs"`
	MaxVUs          null.Int `json:"maxVUs"`
}

// NewVariableArrivalRateConfig returns a VariableArrivalRateConfig with default values
func NewVariableArrivalRateConfig(name string) VariableArrivalRateConfig {
	return VariableArrivalRateConfig{
		BaseConfig: NewBaseConfig(name, variableArrivalRateType),
		TimeUnit:   types.NewNullDuration(1*time.Second, false),
	}
}

// Make sure we implement the lib.ExecutorConfig interface
var _ lib.ExecutorConfig = &VariableArrivalRateConfig{}

// GetPreAllocatedVUs is just a helper method that returns the scaled pre-allocated VUs.
func (varc VariableArrivalRateConfig) GetPreAllocatedVUs(es *lib.ExecutionSegment) int64 {
	return es.Scale(varc.PreAllocatedVUs.Int64)
}

// GetMaxVUs is just a helper method that returns the scaled max VUs.
func (varc VariableArrivalRateConfig) GetMaxVUs(es *lib.ExecutionSegment) int64 {
	return es.Scale(varc.MaxVUs.Int64)
}

// GetDescription returns a human-readable description of the executor options
func (varc VariableArrivalRateConfig) GetDescription(es *lib.ExecutionSegment) string {
	//TODO: something better? always show iterations per second?
	maxVUsRange := fmt.Sprintf("maxVUs: %d", es.Scale(varc.PreAllocatedVUs.Int64))
	if varc.MaxVUs.Int64 > varc.PreAllocatedVUs.Int64 {
		maxVUsRange += fmt.Sprintf("-%d", es.Scale(varc.MaxVUs.Int64))
	}
	maxUnscaledRate := getStagesUnscaledMaxTarget(varc.StartRate.Int64, varc.Stages)
	maxArrRatePerSec, _ := getArrivalRatePerSec(
		getScaledArrivalRate(es, maxUnscaledRate, time.Duration(varc.TimeUnit.Duration)),
	).Float64()

	return fmt.Sprintf("Up to %.2f iterations/s for %s over %d stages%s",
		maxArrRatePerSec, sumStagesDuration(varc.Stages),
		len(varc.Stages), varc.getBaseInfo(maxVUsRange))
}

// Validate makes sure all options are configured and valid
func (varc VariableArrivalRateConfig) Validate() []error {
	errors := varc.BaseConfig.Validate()

	if varc.StartRate.Int64 < 0 {
		errors = append(errors, fmt.Errorf("the startRate value shouldn't be negative"))
	}

	if time.Duration(varc.TimeUnit.Duration) < 0 {
		errors = append(errors, fmt.Errorf("the timeUnit should be more than 0"))
	}

	errors = append(errors, validateStages(varc.Stages)...)

	if !varc.PreAllocatedVUs.Valid {
		errors = append(errors, fmt.Errorf("the number of preAllocatedVUs isn't specified"))
	} else if varc.PreAllocatedVUs.Int64 < 0 {
		errors = append(errors, fmt.Errorf("the number of preAllocatedVUs shouldn't be negative"))
	}

	if !varc.MaxVUs.Valid {
		errors = append(errors, fmt.Errorf("the number of maxVUs isn't specified"))
	} else if varc.MaxVUs.Int64 < varc.PreAllocatedVUs.Int64 {
		errors = append(errors, fmt.Errorf("maxVUs shouldn't be less than preAllocatedVUs"))
	}

	return errors
}

// GetExecutionRequirements returns the number of required VUs to run the
// executor for its whole duration (disregarding any startTime), including the
// maximum waiting time for any iterations to gracefully stop. This is used by
// the execution scheduler in its VU reservation calculations, so it knows how
// many VUs to pre-initialize.
func (varc VariableArrivalRateConfig) GetExecutionRequirements(es *lib.ExecutionSegment) []lib.ExecutionStep {
	return []lib.ExecutionStep{
		{
			TimeOffset:      0,
			PlannedVUs:      uint64(es.Scale(varc.PreAllocatedVUs.Int64)),
			MaxUnplannedVUs: uint64(es.Scale(varc.MaxVUs.Int64 - varc.PreAllocatedVUs.Int64)),
		},
		{
			TimeOffset:      sumStagesDuration(varc.Stages) + time.Duration(varc.GracefulStop.Duration),
			PlannedVUs:      0,
			MaxUnplannedVUs: 0,
		},
	}
}

// NewExecutor creates a new VariableArrivalRate executor
func (varc VariableArrivalRateConfig) NewExecutor(
	es *lib.ExecutionState, logger *logrus.Entry,
) (lib.Executor, error) {
	return VariableArrivalRate{
		BaseExecutor: NewBaseExecutor(varc, es, logger),
		config:       varc,
	}, nil
}

// HasWork reports whether there is any work to be done for the given execution segment.
func (varc VariableArrivalRateConfig) HasWork(es *lib.ExecutionSegment) bool {
	return varc.GetMaxVUs(es) > 0
}

// VariableArrivalRate tries to execute a specific number of iterations for a
// specific period.
//TODO: combine with the ConstantArrivalRate?
type VariableArrivalRate struct {
	*BaseExecutor
	config VariableArrivalRateConfig
}

// Make sure we implement the lib.Executor interface.
var _ lib.Executor = &VariableArrivalRate{}

var two big.Rat

func init() {
	two.SetInt64(2)
}

// from https://groups.google.com/forum/#!topic/golang-nuts/aIcDf8T-Png
func sqrtRat(x *big.Rat) *big.Rat {
	var z, a, b big.Rat
	var ns, ds big.Int
	ni, di := x.Num(), x.Denom()
	z.SetFrac(ns.Rsh(ni, uint(ni.BitLen())/2), ds.Rsh(di, uint(di.BitLen())/2))
	for i := 10; i > 0; i-- { //TODO: better termination
		a.Sub(a.Mul(&z, &z), x)
		f, _ := a.Float64()
		if f == 0 {
			break
		}
		fmt.Println(x, z, i)
		z.Sub(&z, b.Quo(&a, b.Mul(&two, &z)))
	}
	return &z
}

// This implementation is just for reference
// TODO: add test to check that `cal` is accurate enough ...
func (varc VariableArrivalRateConfig) calRat(ch chan<- time.Duration) {
	defer close(ch)
	curr := varc.StartRate.ValueOrZero()
	var base time.Duration = 0
	for _, stage := range varc.Stages {
		// fmt.Println(stage)
		target := stage.Target.ValueOrZero()
		if target != curr {
			var (
				a = big.NewRat(curr, int64(time.Second))
				b = big.NewRat(target, int64(time.Second))
				c = big.NewRat(time.Duration(stage.Duration.Duration).Nanoseconds(), 1)
			)
			i := int64(1)
			for ; ; i++ {
				// a - b!=0, x = (a c - sqrt(c (a^2 c - 2 a d + 2 b d)))/(a - b), c!=0
				x := new(big.Rat).Mul( // divide
					new(big.Rat).Sub( // -
						new(big.Rat).Mul(a, c), // a *c
						sqrtRat( // sqrt (c * (c*a^2 + 2d*(b-a)))
							new(big.Rat).Mul(
								c,
								new(big.Rat).Add(
									new(big.Rat).Mul(c, new(big.Rat).Mul(a, a)),
									new(big.Rat).Mul(big.NewRat(2*i, 1), new(big.Rat).Sub(b, a)),
								)))),
					new(big.Rat).Inv(new(big.Rat).Sub(a, b))) // a - b
				fmt.Println(a, b, c, i, x.FloatString(50))
				if x.Cmp(c) > 0 {
					// fmt.Println(x.Sub(x, c))
					break
				}
				r, _ := x.Float64()
				ch <- base + time.Duration(r)
			}
		} else {
			step := big.NewRat(int64(time.Second), target)
			a := big.NewRat(int64(time.Second), target)
			c := big.NewRat(time.Duration(stage.Duration.Duration).Nanoseconds(), 1)
			for { // TODO: remove the 50
				if a.Cmp(c) > 0 {
					break
				}
				// fmt.Println(a, step)
				r, _ := a.Float64()
				ch <- base + time.Duration(r)
				a.Add(a, step)
			}
		}
		base += time.Duration(stage.Duration.Duration)
		curr = target
		// fmt.Println("end ", stage)
	}
}

const epsilon = 0.000_000_000_1 // this 1/10 nanosecond ... so we don' care at that point

func (varc VariableArrivalRateConfig) cal(ch chan<- time.Duration) {
	// TODO: add inline comments with explanation of what is happening
	// for now just link to https://github.com/loadimpact/k6/issues/1299#issuecomment-575661084
	defer close(ch)
	var base time.Duration
	curr := varc.StartRate.ValueOrZero()
	var carry float64
	for _, stage := range varc.Stages {
		// TODO remove the fmt.Println debug helpers :D
		// fmt.Println(stage)
		target := stage.Target.ValueOrZero()
		if target != curr {
			var (
				a        = float64(curr) / float64(time.Second)
				b        = float64(target) / float64(time.Second)
				c        = float64(time.Duration(stage.Duration.Duration).Nanoseconds())
				x        float64
				endCount = c * ((b-a)/2 + a)
				i        = float64(1)
			)

			if carry != 0 {
				i -= carry
			}

			// fmt.Printf("%f %f %f\n", i, endCount, carry)
			for ; i <= endCount; i++ {
				// fmt.Println(i, endCount)
				x = (a*c - math.Sqrt(c*(a*a*c+2*i*(b-a)))) / (a - b)
				// fmt.Printf("%.10f, %.10f, %.10f, %.10f, %f\n", a, b, c, i, x)

				if math.IsNaN(x) {
					fmt.Printf("break, %f,%f\n", x, c)
					break
				}
				// fmt.Println("x=", time.Duration(x), base)
				ch <- time.Duration(x) + base
			}
			carry = endCount - (i - 1)
		} else {
			var (
				a     = float64(time.Second) / float64(target)
				step  = a
				c     = float64(time.Duration(stage.Duration.Duration).Nanoseconds())
				count = c / float64(time.Duration(varc.TimeUnit.Duration).Nanoseconds()) * float64(target)
				i     = float64(1)
			)
			if carry != 0 {
				i -= carry
				a -= a * carry
			}
			// fmt.Printf("%f %f %f\n", a, carry, a*carry)
			// a -= a * carry

			for ; i <= count; i++ {
				// fmt.Println(time.Duration(a), time.Duration(step))
				ch <- time.Duration(a) + base
				a += step
			}
		}
		curr = target
		base += time.Duration(stage.Duration.Duration)
		// fmt.Println("end ", stage)
	}
}

// Run executes a variable number of iterations per second.
func (varr VariableArrivalRate) Run(ctx context.Context, out chan<- stats.SampleContainer) (err error) { //nolint:funlen
	segment := varr.executionState.Options.ExecutionSegment
	gracefulStop := varr.config.GetGracefulStop()
	duration := sumStagesDuration(varr.config.Stages)
	preAllocatedVUs := varr.config.GetPreAllocatedVUs(segment)
	maxVUs := varr.config.GetMaxVUs(segment)

	timeUnit := time.Duration(varr.config.TimeUnit.Duration)
	startArrivalRate := getScaledArrivalRate(segment, varr.config.StartRate.Int64, timeUnit)

	maxUnscaledRate := getStagesUnscaledMaxTarget(varr.config.StartRate.Int64, varr.config.Stages)
	maxArrivalRatePerSec, _ := getArrivalRatePerSec(getScaledArrivalRate(segment, maxUnscaledRate, timeUnit)).Float64()
	startTickerPeriod := getTickerPeriod(startArrivalRate)

	startTime, maxDurationCtx, regDurationCtx, cancel := getDurationContexts(ctx, duration, gracefulStop)
	defer cancel()

	// Make sure the log and the progress bar have accurate information
	varr.logger.WithFields(logrus.Fields{
		"maxVUs": maxVUs, "preAllocatedVUs": preAllocatedVUs, "duration": duration, "numStages": len(varr.config.Stages),
		"startTickerPeriod": startTickerPeriod.Duration, "type": varr.config.GetType(),
	}).Debug("Starting executor run...")

	// Pre-allocate the VUs local shared buffer
	vus := make(chan lib.VU, maxVUs)

	initialisedVUs := uint64(0)
	// Make sure we put back planned and unplanned VUs back in the global
	// buffer, and as an extra incentive, this replaces a waitgroup.
	defer func() {
		// no need for atomics, since initialisedVUs is mutated only in the select{}
		for i := uint64(0); i < initialisedVUs; i++ {
			varr.executionState.ReturnVU(<-vus, true)
		}
	}()

	// Get the pre-allocated VUs in the local buffer
	for i := int64(0); i < preAllocatedVUs; i++ {
		vu, err := varr.executionState.GetPlannedVU(varr.logger, true)
		if err != nil {
			return err
		}
		initialisedVUs++
		vus <- vu
	}

	tickerPeriod := new(int64)
	*tickerPeriod = int64(startTickerPeriod.Duration)

	fmtStr := pb.GetFixedLengthFloatFormat(maxArrivalRatePerSec, 2) + " iters/s, " +
		pb.GetFixedLengthIntFormat(maxVUs) + " out of " + pb.GetFixedLengthIntFormat(maxVUs) + " VUs active"
	progresFn := func() (float64, string) {
		currentInitialisedVUs := atomic.LoadUint64(&initialisedVUs)
		currentTickerPeriod := atomic.LoadInt64(tickerPeriod)
		vusInBuffer := uint64(len(vus))

		itersPerSec := 0.0
		if currentTickerPeriod > 0 {
			itersPerSec = float64(time.Second) / float64(currentTickerPeriod)
		}
		return math.Min(1, float64(time.Since(startTime))/float64(duration)), fmt.Sprintf(fmtStr,
			itersPerSec, currentInitialisedVUs-vusInBuffer, currentInitialisedVUs,
		)
	}
	varr.progress.Modify(pb.WithProgress(progresFn))
	go trackProgress(ctx, maxDurationCtx, regDurationCtx, varr, progresFn)

	regDurationDone := regDurationCtx.Done()
	runIterationBasic := getIterationRunner(varr.executionState, varr.logger, out)
	runIteration := func(vu lib.VU) {
		runIterationBasic(maxDurationCtx, vu)
		vus <- vu
	}

	remainingUnplannedVUs := maxVUs - preAllocatedVUs

	startIteration := func() error {
		select {
		case vu := <-vus:
			// ideally, we get the VU from the buffer without any issues
			go runIteration(vu)
		default:
			if remainingUnplannedVUs == 0 {
				//TODO: emit an error metric?
				varr.logger.Warningf("Insufficient VUs, reached %d active VUs and cannot allocate more", maxVUs)
				break
			}
			vu, err := varr.executionState.GetUnplannedVU(maxDurationCtx, varr.logger)
			if err != nil {
				return err
			}
			remainingUnplannedVUs--
			atomic.AddUint64(&initialisedVUs, 1)
			go runIteration(vu)
		}
		return nil
	}

	var timer = time.NewTimer(time.Hour)
	var start = time.Now()
	var ch = make(chan time.Duration, 0)
	go varr.config.cal(ch)
	for {
		select {
		case nextTime, ok := <-ch:
			if !ok {
				return nil
			}
			b := time.Until(start.Add(nextTime))
			// fmt.Println(b)
			atomic.StoreInt64(tickerPeriod, int64(b))
			if b < 0 {
				// fmt.Println(time.Now())
				err := startIteration()
				if err != nil {
					return err
				}
				continue
			}
			timer.Reset(b)
			select {
			case <-timer.C:
				// fmt.Println(time.Now())
				err := startIteration()
				if err != nil {
					return err
				}
			case <-regDurationDone:
				return nil
			}
		case <-regDurationDone:
			return nil
		}
	}
}
