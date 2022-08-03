package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type SyncProducer sarama.SyncProducer

type ProducerConfiguration struct {
	Addrs           []string `default:":9091"`
	ShutdownTimeout time.Duration
}

type SyncProducerWrapper struct {
	name         string
	cfg          *ProducerConfiguration
	SyncProducer sarama.SyncProducer
	logger       *zap.Logger
}

func NewSyncProducer(name string, config interface{}, log *zap.Logger) (*SyncProducerWrapper, error) {
	cfg, ok := config.(*ProducerConfiguration)
	if !ok || cfg == nil {
		return nil, errors.New("invalid sync producer config")
	}
	saramaConfig := sarama.NewConfig()
	// Required by SyncProducer
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	syncProducer, err := sarama.NewSyncProducer(cfg.Addrs, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &SyncProducerWrapper{
		name:         name,
		cfg:          cfg,
		SyncProducer: syncProducer,
		logger:       log,
	}, nil
}

func (w *SyncProducerWrapper) Start(context.Context) error {
	return nil
}

func (w *SyncProducerWrapper) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.ShutdownTimeout)
	defer cancel()

	stop := make(chan error, 1)
	go func() {
		w.logger.Info("closing kafka sync producer")
		stop <- w.SyncProducer.Close()
	}()

	select {
	case e := <-stop:
		if e != nil {
			w.logger.Error("could not close kafka sync producer", zap.Error(e))
			return e
		}
		w.logger.Info("gracefully closed kafka sync producer")
	case <-ctx.Done():
		w.logger.Warn("ShutdownTimeout exceeded while closing kafka sync producer")
		return ctx.Err()
	}
	return nil
}

func (w *SyncProducerWrapper) Name() string {
	return w.name
}
