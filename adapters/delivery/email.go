package exportdelivery

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime/multipart"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

// EmailMessage describes an outbound email delivery.
type EmailMessage struct {
	From       string
	To         []string
	Cc         []string
	Bcc        []string
	ReplyTo    string
	Subject    string
	Body       string
	Attachment *Attachment
}

// EmailSender delivers email messages.
type EmailSender interface {
	Send(ctx context.Context, msg EmailMessage) error
}

// SMTPClient abstracts SMTP delivery.
type SMTPClient interface {
	SendMail(addr string, auth smtp.Auth, from string, to []string, msg []byte) error
}

// SMTPMailer sends email via SMTP.
type SMTPMailer struct {
	Addr   string
	Auth   smtp.Auth
	From   string
	Client SMTPClient
	Now    func() time.Time
}

// Send delivers the message via SMTP.
func (m *SMTPMailer) Send(ctx context.Context, msg EmailMessage) error {
	if m == nil {
		return export.NewError(export.KindInternal, "mailer is nil", nil)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	from := strings.TrimSpace(msg.From)
	if from == "" {
		from = strings.TrimSpace(m.From)
	}
	if from == "" {
		return export.NewError(export.KindValidation, "email from is required", nil)
	}
	if len(msg.To) == 0 && len(msg.Cc) == 0 && len(msg.Bcc) == 0 {
		return export.NewError(export.KindValidation, "email recipients are required", nil)
	}

	payload, err := buildEmailMessage(msg, from, nowOr(m.Now))
	if err != nil {
		return err
	}

	client := m.Client
	if client == nil {
		client = smtpClient{}
	}

	recipients := append(append([]string{}, msg.To...), msg.Cc...)
	recipients = append(recipients, msg.Bcc...)
	if err := client.SendMail(m.Addr, m.Auth, from, recipients, payload); err != nil {
		return export.NewError(export.KindExternal, "smtp send failed", err)
	}
	return nil
}

func buildEmailMessage(msg EmailMessage, from string, now time.Time) ([]byte, error) {
	var buf bytes.Buffer
	writeHeader(&buf, "From", from)
	if len(msg.To) > 0 {
		writeHeader(&buf, "To", strings.Join(msg.To, ", "))
	}
	if len(msg.Cc) > 0 {
		writeHeader(&buf, "Cc", strings.Join(msg.Cc, ", "))
	}
	if reply := strings.TrimSpace(msg.ReplyTo); reply != "" {
		writeHeader(&buf, "Reply-To", reply)
	}
	writeHeader(&buf, "Subject", msg.Subject)
	writeHeader(&buf, "Date", now.Format(time.RFC1123Z))
	writeHeader(&buf, "MIME-Version", "1.0")

	if msg.Attachment == nil {
		writeHeader(&buf, "Content-Type", "text/plain; charset=utf-8")
		writeHeader(&buf, "Content-Transfer-Encoding", "7bit")
		buf.WriteString("\r\n")
		buf.WriteString(msg.Body)
		buf.WriteString("\r\n")
		return buf.Bytes(), nil
	}

	writer := multipart.NewWriter(&buf)
	writeHeader(&buf, "Content-Type", fmt.Sprintf("multipart/mixed; boundary=%q", writer.Boundary()))
	buf.WriteString("\r\n")

	textHeaders := make(textproto.MIMEHeader)
	textHeaders.Set("Content-Type", "text/plain; charset=utf-8")
	textHeaders.Set("Content-Transfer-Encoding", "7bit")
	textPart, err := writer.CreatePart(textHeaders)
	if err != nil {
		return nil, err
	}
	if _, err := textPart.Write([]byte(msg.Body)); err != nil {
		return nil, err
	}

	attachHeaders := make(textproto.MIMEHeader)
	contentType := msg.Attachment.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	attachHeaders.Set("Content-Type", contentType)
	attachHeaders.Set("Content-Transfer-Encoding", "base64")
	attachHeaders.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", msg.Attachment.Filename))
	attachPart, err := writer.CreatePart(attachHeaders)
	if err != nil {
		return nil, err
	}
	if err := writeBase64(attachPart, msg.Attachment.Data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeHeader(buf *bytes.Buffer, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	buf.WriteString(key)
	buf.WriteString(": ")
	buf.WriteString(value)
	buf.WriteString("\r\n")
}

func writeBase64(w io.Writer, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	for len(encoded) > 76 {
		if _, err := w.Write([]byte(encoded[:76] + "\r\n")); err != nil {
			return err
		}
		encoded = encoded[76:]
	}
	if len(encoded) > 0 {
		if _, err := w.Write([]byte(encoded + "\r\n")); err != nil {
			return err
		}
	}
	return nil
}

func nowOr(nowFn func() time.Time) time.Time {
	if nowFn != nil {
		return nowFn()
	}
	return time.Now()
}

type smtpClient struct{}

func (smtpClient) SendMail(addr string, auth smtp.Auth, from string, to []string, msg []byte) error {
	return smtp.SendMail(addr, auth, from, to, msg)
}
