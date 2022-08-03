package mongodb

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type ClientConfiguration struct {
	Address         string        `default:"mongodb://localhost:27017"`
	User            string        `default:"admin"`
	Password        string        `default:"admin"`
	ConnectTimeout  time.Duration `default:"30s"`
	MaxConnIdleTime time.Duration `default:"30m"`
	MinPoolSize     uint64        `default:"20"`
	MaxPoolSize     uint64        `default:"300"`
}

type ClientWrapper struct {
	name   string
	cfg    *ClientConfiguration
	Client *mongo.Client
	logger *zap.Logger
}

func NewClient(name string, config any, log *zap.Logger) (*ClientWrapper, error) {
	cfg, ok := config.(*ClientConfiguration)
	if !ok || cfg == nil {
		return nil, errors.New("invalid mongodb client config")
	}
	client, err := mongo.NewClient(
		options.Client().ApplyURI(cfg.Address).
			SetAuth(options.Credential{
				Username: cfg.User,
				Password: cfg.Password,
			}).
			SetConnectTimeout(cfg.ConnectTimeout).
			SetMaxConnIdleTime(cfg.MaxConnIdleTime).
			SetMinPoolSize(cfg.MinPoolSize).
			SetMaxPoolSize(cfg.MaxPoolSize))

	if err != nil {
		return nil, err
	}
	return &ClientWrapper{
		name:   name,
		cfg:    cfg,
		Client: client,
		logger: log,
	}, nil
}

func (w *ClientWrapper) Start(ctx context.Context) error {
	w.logger.Info("connecting mongo client", zap.String("addr", w.cfg.Address))
	if err := w.Client.Connect(ctx); err != nil {
		return err
	}

	if err := w.Client.Ping(ctx, nil); err != nil {
		return err
	}

	return nil
}

func (w *ClientWrapper) Stop(ctx context.Context) error {
	w.logger.Info("disconnecting mongo client", zap.String("addr", w.cfg.Address))
	return w.Client.Disconnect(ctx)
}

func (w *ClientWrapper) Name() string {
	return w.name
}
