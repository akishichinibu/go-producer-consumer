package proconsumer

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

type proConsumer[I, O any] struct {
	Produce    Producer[I]
	Consume    Consumer[I, O]
	OnFailed   func(error) error
	Worker     int
	BufferSize int

	poolProducer sync.Pool
	poolConsumer sync.Pool
	failedBuffer chan error
	buffers      []chan I
}

func (p *proConsumer[I, O]) produce(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		input := p.poolProducer.Get().(*I)
		err := p.Produce(ctx, input)
		if err != nil {
			if err2 := p.OnFailed(err); err2 != nil {
				p.failedBuffer <- err2
				return err2
			}
		}
	}
	return nil
}

func (p *proConsumer[I, O]) Process(ctx context.Context) error {
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(2 + p.Worker)

	g.Go(func() error {
		return p.produce(ctx)
	})

	g.Go(func() error {
		// for {
		// 	select {
		// 	case <-ctx.Done():
		// 		return nil
		// 	case input := <-p.buffers[i]:
		// 		output := p.poolConsumer.Get().(*O)
		// 		err := p.Consume(ctx, &input, output)
		// 		if err != nil {
		// 			if err2 := p.OnFailed(err); err2 != nil {
		// 				p.failedBuffer <- err2
		// 				return err2
		// 			}
		// 		}
		// 	}
		// }
		return nil
	})

	for err := range p.failedBuffer {
		if err == nil {
			continue
		}
		close(p.failedBuffer)
		for i := 0; i < p.Worker; i++ {
			close(p.buffers[i])
		}
		return err
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
