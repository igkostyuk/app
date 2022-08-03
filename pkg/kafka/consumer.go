package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type Handler interface {
	Handle(context.Context, *sarama.ConsumerMessage) error
}

type ConsumeGroupConfiguration struct {
	StartupTimeout  time.Duration `default:"10s"`
	ShutdownTimeout time.Duration
	Topics          []string `default:"test"`
	Brokers         []string `default:":9091"`
	GroupID         string   `default:"test"`
}

type ConsumerWrapper struct {
	name string
	sync.RWMutex
	cfg            *ConsumeGroupConfiguration
	consumerGroup  sarama.ConsumerGroup
	tracerProvider trace.TracerProvider
	logger         *zap.Logger
	ready          chan struct{}
	Handler        Handler
	cancel         context.CancelFunc

	// WaitGroup which returns when all goroutines started in Start() have finished.
	// Used to ensure all goroutines have finished as part of graceful Stop().
	startWg sync.WaitGroup
}

// NewConsumerGroup will return a configured sarama.ConsumerGroup.
func NewConsumerGroup(name string, config interface{}, log *zap.Logger) (*ConsumerWrapper, error) {
	cfg, ok := config.(*ConsumeGroupConfiguration)
	if !ok || cfg == nil {
		return nil, errors.New("invalid consumer group config")
	}
	saramaConfig := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		return nil, err
	}

	return &ConsumerWrapper{
		name:          name,
		cfg:           cfg,
		consumerGroup: consumerGroup,
		logger:        log,
		ready:         make(chan struct{}),
	}, nil
}

func (w *ConsumerWrapper) Start(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Exit if the consumer group cannot be started within the configured time.
	timeout := time.NewTimer(w.cfg.StartupTimeout)
	defer timeout.Stop()

	// Setup a CancelFunc to notify goroutines to quit.
	ctx, cancel := context.WithCancel(ctx)
	w.Lock()
	w.cancel = cancel
	w.Unlock()

	// Wrap the handler for instrumentation.
	handler := otelsarama.WrapConsumerGroupHandler(w)

	// Track errors
	w.startWg.Add(1)
	go func() {
		defer w.startWg.Done()
		for err := range w.consumerGroup.Errors() {
			w.logger.Error("consumer group", zap.Error(err))
		}
		w.logger.Info("consumer group errors drained")
	}()

	// Consume claims.
	w.startWg.Add(1)
	go func() {
		defer w.startWg.Done()
		w.logger.Info("starting consumer group")
		for {
			// Check if context was cancelled, signaling that the consumer should stop.
			if ctx.Err() != nil {
				w.logger.Error("context canceled while starting consumer group", zap.Error(ctx.Err()))
				return
			}
			// `Consume` should be called inside an infinite loop.
			err := w.consumerGroup.Consume(ctx, w.cfg.Topics, handler)
			if err != nil {
				w.logger.Error("consumer group session ended abnormally", zap.Error(err))
				return
			}

			// The consumer group session successfully closed. A new session will be created
			// on the next iteration of the loop.
			w.logger.Info("consumer group session closed")
		}
	}()

	// Wait for the consumer group to be setup.
	select {
	case <-w.ready:
		w.logger.Info("consumer group up and running")
		return nil
	case <-ctx.Done():
		w.logger.Error("context canceled while starting consumer group", zap.Error(ctx.Err()))
		return ctx.Err()
	case <-timeout.C:
		w.logger.Error("deadline exceeded for starting consumer group", zap.Error(context.DeadlineExceeded))
		return context.DeadlineExceeded
	}
}

func (w *ConsumerWrapper) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.ShutdownTimeout)
	defer cancel()

	// Buffered stop channel in case context completes - we don't want to leak goroutine here
	stop := make(chan error, 1)
	go func() {
		w.logger.Info("closing kafka consumer group")
		err := w.consumerGroup.Close()
		w.logger.Info("waiting for goroutines to stop")
		w.startWg.Wait()
		stop <- err
	}()

	select {
	case e := <-stop:
		if e != nil {
			w.logger.Error("could not close kafka consumer group", zap.Error(e))
			return e
		}
		w.logger.Info("gracefully closed kafka consumer group")
	case <-ctx.Done():
		w.logger.Warn("ShutdownTimeout exceeded while closing kafka consumer group")
		w.cancel()
		return ctx.Err()
	}
	return nil
}

func (w *ConsumerWrapper) Setup(session sarama.ConsumerGroupSession) error {
	w.Lock()
	defer w.Unlock()
	close(w.ready)

	w.logger.With(
		zap.String("kafka.session.member_id", session.MemberID()),
		zap.Int32("kafka.session.generation_id", session.GenerationID()),
	).Info("setup consumer group session")

	return nil
}

func (w *ConsumerWrapper) Cleanup(session sarama.ConsumerGroupSession) error {
	w.Lock()
	defer w.Unlock()
	// Prepare the ready channel for the next session.
	w.ready = make(chan struct{})

	w.logger.With(
		zap.String("kafka.session.member_id", session.MemberID()),
		zap.Int32("kafka.session.generation_id", session.GenerationID()),
	).Info("cleanup consumer group session")

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (w *ConsumerWrapper) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	l := w.logger.With(zap.String("kafka.session.member_id", session.MemberID()),
		zap.Int32("kafka.session.generation_id", session.GenerationID()))

	l.Info("Starting a ConsumeClaim")
	defer l.Info("ConsumeClaim finished")

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for {
		select {
		case <-session.Context().Done():
			l.Info("received session context done signal, exiting consume claim")
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := w.Handler.Handle(session.Context(), message); err != nil {
				w.logger.With(
					zap.Int64("offset", message.Offset),
					zap.Int32("partition", message.Partition),
					zap.Time("timestamp", message.Timestamp),
					zap.Time("block_timestamp", message.BlockTimestamp),
					zap.ByteString("key", message.Key),
					zap.String("topic", message.Topic),
					zap.Error(err),
				).Error("consume claim: process message: could not process message")
			}

			session.MarkMessage(message, "")
		}
	}
}

func (w *ConsumerWrapper) Name() string {
	return w.name
}
