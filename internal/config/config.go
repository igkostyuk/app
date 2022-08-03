package config

import (
	"github.com/igkostyuk/app/pkg/grpc"
	"github.com/igkostyuk/app/pkg/kafka"
	"github.com/igkostyuk/app/pkg/mail"
)

type Config struct {
	Server     *grpc.ServerConfiguration
	Client     *grpc.ClientConfiguration
	Producer   *kafka.ProducerConfiguration
	Consumer   *kafka.ConsumeGroupConfiguration
	MailSender *mail.MailConfiguration

	FrontEndServiceBaseURL string `envconfig:"FRONT_END_BASE_URL" default:"http://localhost:8080"`
}
