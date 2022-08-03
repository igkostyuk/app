package mail

import (
	"context"
	"errors"
	"fmt"
	"net/smtp"
	"strings"

	"go.uber.org/zap"
)

type MailConfiguration struct {
	Host     string `envconfig:"MAIL_HOST"`
	Port     string `envconfig:"MAIL_PORT"`
	User     string `envconfig:"MAIL_USER"`
	Password string `envconfig:"MAIL_PASS"`
	Sender   string `envconfig:"MAIL_SENDER" default:"result-collector@example.com"`
}

type MailSender struct {
	name     string
	host     string
	port     string
	user     string
	password string
	sender   string
	logger   *zap.Logger
}

type Mail struct {
	Sender  string
	To      []string
	Subject string
	Body    string
}

func NewMailSender(name string, config interface{}, log *zap.Logger) (*MailSender, error) {
	cfg, ok := config.(*MailConfiguration)
	if !ok || cfg == nil {
		return nil, errors.New("invalid mail sender config")
	}
	return &MailSender{
		name:     name,
		logger:   log,
		host:     cfg.Host,
		port:     cfg.Port,
		user:     cfg.User,
		password: cfg.Password,
		sender:   cfg.Sender,
	}, nil
}

func (s *MailSender) Send(ctx context.Context, mail Mail) error {
	mail.Sender = s.sender

	addr := fmt.Sprintf("%s:%s", s.host, s.port)
	msg := BuildMessage(mail)
	auth := smtp.PlainAuth("", s.user, s.password, s.host)
	err := smtp.SendMail(addr, auth, s.sender, mail.To, []byte(msg))

	if err != nil {
		return err
	}

	fmt.Println("Email sent successfully")
	return nil
}

func BuildMessage(mail Mail) string {
	msg := "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\r\n"
	msg += fmt.Sprintf("From: %s\r\n", mail.Sender)
	msg += fmt.Sprintf("To: %s\r\n", strings.Join(mail.To, ";"))
	msg += fmt.Sprintf("Subject: %s\r\n", mail.Subject)
	msg += fmt.Sprintf("\r\n%s\r\n", mail.Body)

	return msg
}
