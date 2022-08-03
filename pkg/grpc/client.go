package grpc

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ClientConfiguration struct {
	Target string
}

type ClientWrapper struct {
	name       string
	cfg        *ClientConfiguration
	ClientConn *grpc.ClientConn
	logger     *zap.Logger
}

func NewClient(name string, config interface{}, log *zap.Logger) (*ClientWrapper, error) {
	cfg, ok := config.(*ClientConfiguration)
	if !ok || cfg == nil {
		return nil, errors.New("invalid client config")
	}
	conn, err := grpc.Dial(cfg.Target, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &ClientWrapper{
		name:       name,
		cfg:        cfg,
		ClientConn: conn,
		logger:     log,
	}, nil
}

func (w *ClientWrapper) Start(context.Context) error {
	return nil
}

func (w *ClientWrapper) Stop(context.Context) error {
	w.logger.Info("closing grpc client", zap.String("target", w.cfg.Target))
	return w.ClientConn.Close()
}

func (w *ClientWrapper) Name() string {
	return w.name
}
