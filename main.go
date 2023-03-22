package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type slot struct{}

type JobID string

type Job interface {
	Id() JobID
	Execute(ctx context.Context) Result
}
type Result struct {
	Id    JobID
	Err   error
	Value interface{}
}

type WorkerPool struct {
	workersCount int
	jobs         chan Job
	results      chan Result
	Done         chan struct{}
}

type Test struct {
	id          JobID
	timeToSleep time.Duration
}

func (t *Test) Execute(ctx context.Context) Result {
	_, cancel := context.WithCancel(ctx)
	defer cancel()
	time.Sleep(t.timeToSleep)
	return Result{Id: t.id, Err: nil, Value: fmt.Sprintf("Hello from %v", t.id)}
}

func (t *Test) Id() JobID {
	return t.id
}

func NewTest(id JobID, t time.Duration) *Test {
	return &Test{id, t}
}

func NewWorkerPool(wcount int) WorkerPool {
	return WorkerPool{
		workersCount: wcount,
		jobs:         make(chan Job, wcount),
		results:      make(chan Result, wcount),
		Done:         make(chan struct{}),
	}
}

func (wp WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < wp.workersCount; i++ {
		wg.Add(1)
		// fan out worker goroutines
		//reading from jobs channel and
		//pushing calcs into results channel
		go worker(ctx, &wg, wp.jobs, wp.results)
	}

	wg.Wait()
	close(wp.Done)
	close(wp.results)
}

func worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan Job, results chan<- Result) {
	defer wg.Done()
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				return
			}
			// fan-in job execution multiplexing results into the results channel
			results <- job.Execute(ctx)
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v\n", ctx.Err())
			results <- Result{
				Err: ctx.Err(),
			}
			return
		}
	}
}

func main() {
	wp := NewWorkerPool(5)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*1)
	defer cancel()

	go func() {
		for i := 1; i <= 10; i++ {
			wp.jobs <- NewTest(JobID(strconv.Itoa(i)), 3*time.Second)
		}
		close(wp.jobs)
	}()

	go wp.Run(ctx)

	for {
		select {
		case r := <-wp.results:
			if r.Err != nil && r.Err != context.DeadlineExceeded {
				fmt.Errorf("expected error: %v; got: %v", context.DeadlineExceeded, r.Err)
			} else {
				fmt.Println(r.Value)
			}
		case <-wp.Done:
			return
		}
	}

}
