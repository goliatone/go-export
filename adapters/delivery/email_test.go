package exportdelivery

import (
	"context"
	"net/smtp"
	"strings"
	"testing"
)

type captureSMTP struct {
	addr string
	from string
	to   []string
	msg  []byte
}

func (c *captureSMTP) SendMail(addr string, _ smtp.Auth, from string, to []string, msg []byte) error {
	c.addr = addr
	c.from = from
	c.to = append([]string{}, to...)
	c.msg = append([]byte{}, msg...)
	return nil
}

func TestSMTPMailer_SendWithAttachment(t *testing.T) {
	client := &captureSMTP{}
	mailer := &SMTPMailer{
		Addr:   "smtp.test:25",
		From:   "sender@example.com",
		Client: client,
	}

	err := mailer.Send(context.Background(), EmailMessage{
		To:      []string{"recipient@example.com"},
		Subject: "Report",
		Body:    "Here is your report",
		Attachment: &Attachment{
			Filename:    "report.pdf",
			ContentType: "application/pdf",
			Data:        []byte("pdf"),
		},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	payload := string(client.msg)
	if !strings.Contains(payload, "multipart/mixed") {
		t.Fatalf("expected multipart email")
	}
	if !strings.Contains(payload, "Content-Disposition: attachment") {
		t.Fatalf("expected attachment header")
	}
}

func TestSMTPMailer_SendPlainText(t *testing.T) {
	client := &captureSMTP{}
	mailer := &SMTPMailer{
		Addr:   "smtp.test:25",
		From:   "sender@example.com",
		Client: client,
	}

	err := mailer.Send(context.Background(), EmailMessage{
		To:      []string{"recipient@example.com"},
		Subject: "Report",
		Body:    "Here is your report",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	payload := string(client.msg)
	if !strings.Contains(payload, "Content-Type: text/plain") {
		t.Fatalf("expected text/plain email")
	}
	if strings.Contains(payload, "multipart/mixed") {
		t.Fatalf("did not expect multipart email")
	}
}
