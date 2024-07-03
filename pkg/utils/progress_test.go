package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func TestProgressBar(t *testing.T) {
	p := NewProgress(true, true)
	bar := p.AddCountBar("Bar", 1000)
	cp := p.AddCountSpinner("Spinner")
	bp := p.AddByteSpinner("Spinner")
	bar.SetTotal(50)
	for i := 0; i < 100; i++ {
		time.Sleep(time.Millisecond * 10)
		bar.Increment()
		if i%2 == 0 {
			bar.IncrTotal(1)
			cp.Increment()
			bp.IncrInt64(1024)
		}
	}
	bar.Done()
	p.Done()
	if bar.Current() != 100 || cp.Current() != 50 || bp.Current() != 50*1024 {
		t.Fatalf("Final values: bar %d, count %d, bytes: %d", bar.Current(), cp.Current(), bp.Current())
	}

	p = NewProgress(true, true)
	dp := p.AddDoubleSpinner("Spinner")
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond)
			dp.IncrInt64(1024)
		}
		dp.Done()
	}()
	p.Wait()
	if c, b := dp.Current(); c != 100 || b != 102400 {
		t.Fatalf("Final values: count %d, bytes %d", c, b)
	}
}

func TestProgressBar2(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	// pass &wg (optional), so p will wait for it eventually
	p := mpb.New(mpb.WithWaitGroup(&wg))
	total, numBars := 100, 3
	wg.Add(numBars)

	for i := 0; i < numBars; i++ {
		name := fmt.Sprintf("Bar#%d:", i)
		bar := p.AddBar(int64(total),
			mpb.PrependDecorators(
				// simple name decorator
				decor.Name(name),
				// decor.DSyncWidth bit enables column width synchronization
				decor.Percentage(decor.WCSyncSpace),
			),
			mpb.AppendDecorators(
				// replace ETA decorator with "done" message, OnComplete event
				decor.OnComplete(
					// ETA decorator with ewma age of 60
					decor.EwmaETA(decor.ET_STYLE_GO, 60), "done",
				),
			),
		)
		// simulating some work
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			max := 100 * time.Millisecond
			for i := 0; i < total; i++ {
				// start variable is solely for EWMA calculation
				// EWMA's unit of measure is an iteration's duration
				start := time.Now()
				time.Sleep(time.Duration(rng.Intn(10)+1) * max / 10)
				bar.Increment()
				// we need to call DecoratorEwmaUpdate to fulfill ewma decorator's contract
				bar.DecoratorEwmaUpdate(time.Since(start))
			}
		}()
	}
	// Waiting for passed &wg and for all bars to complete and flush
	p.Wait()

}
