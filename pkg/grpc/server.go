package grpc

import (
	"context"
	"errors"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ServerConfiguration struct {
	Addr            string
	ShutdownTimeout time.Duration
}

type ServerWrapper struct {
	name   string
	cfg    *ServerConfiguration
	Server *grpc.Server
	logger *zap.Logger
}

func NewServer(name string, config interface{}, log *zap.Logger) (*ServerWrapper, error) {
	cfg, ok := config.(*ServerConfiguration)
	if !ok || cfg == nil {
		return nil, errors.New("invalid server config")
	}
	srv := grpc.NewServer()
	return &ServerWrapper{
		name:   name,
		cfg:    cfg,
		Server: srv,
		logger: log,
	}, nil
}

func (w *ServerWrapper) Start(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	w.logger.Info("starting grpc listener", zap.String("addr", w.cfg.Addr))
	lis, err := net.Listen("tcp", w.cfg.Addr)
	if err != nil {
		return err
	}

	go func() {
		w.logger.Info("starting grpc server")
		if err := w.Server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			w.logger.Error("grpc server failed", zap.Error(err))
		}
	}()
	return nil
}

func (w *ServerWrapper) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.ShutdownTimeout)
	defer cancel()
	grpcStopCh := make(chan struct{})
	go func() {
		w.logger.Info("gracefully shutting down grpc server")
		w.Server.GracefulStop()
		close(grpcStopCh)
	}()
	select {
	case <-grpcStopCh:
		w.logger.Info("gracefully shut down grpc server")
	case <-ctx.Done():
		w.logger.Warn("stopping grpc server")
		w.Server.Stop()
		<-grpcStopCh
		grpcStopCh = nil
	}
	return nil
}

func (w *ServerWrapper) Name() string {
	return w.name
}
