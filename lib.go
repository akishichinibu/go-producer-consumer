package proconsumer

import (
	"context"
	"sync"
)

type ProComumer[I, O any] interface {
	Process(context.Context) error
}

type Producer[T any] func(context.Context, *T) error
type Consumer[I, O any] func(context.Context, *I, *O) error

type NewProConsumerParams[I, O any] struct {
	Produce    Producer[I]
	Consume    Consumer[I, O]
	OnFailed   func(error) error
	Worker     int
	BufferSize int
}

func NewProConsumer[I, O any](params NewProConsumerParams[I, O]) ProComumer[I, O] {
	if params.Worker < 1 {
		params.Worker = 16
	}
	if params.BufferSize < 1 {
		params.BufferSize = 1024
	}
	buffers := make([]chan I, params.Worker)
	for i := 0; i < params.Worker; i++ {
		buffers[i] = make(chan I, params.BufferSize)
	}
	return &proConsumer[I, O]{
		Produce:  params.Produce,
		Consume:  params.Consume,
		OnFailed: params.OnFailed,
		Worker:   params.Worker,

		poolProducer: sync.Pool{
			New: func() any {
				return new(I)
			},
		},
		poolConsumer: sync.Pool{
			New: func() any {
				return new(O)
			},
		},
		failedBuffer: make(chan error, 1),
		buffers:      buffers,
	}
}
