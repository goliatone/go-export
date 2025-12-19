package examples

import (
	gcmd "github.com/goliatone/go-command"
	"github.com/goliatone/go-command/dispatcher"
	"github.com/goliatone/go-errors"
	exportcmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	exportqry "github.com/goliatone/go-export/query"
)

// RegisterExportHandlers wires go-export commands and queries to go-command.
func RegisterExportHandlers(reg *gcmd.Registry, svc export.Service) ([]dispatcher.Subscription, error) {
	if svc == nil {
		return nil, errors.New("export service is required", errors.CategoryValidation).
			WithTextCode("SERVICE_REQUIRED")
	}

	req := exportcmd.NewRequestExportHandler(svc)
	cancel := exportcmd.NewCancelExportHandler(svc)
	del := exportcmd.NewDeleteExportHandler(svc)
	gen := exportcmd.NewGenerateExportHandler(svc)
	cleanup := exportcmd.NewCleanupExportsHandler(svc)

	status := exportqry.NewExportStatusHandler(svc)
	history := exportqry.NewExportHistoryHandler(svc)
	download := exportqry.NewDownloadMetadataHandler(svc)

	subscriptions := []dispatcher.Subscription{
		dispatcher.SubscribeCommand(req),
		dispatcher.SubscribeCommand(cancel),
		dispatcher.SubscribeCommand(del),
		dispatcher.SubscribeCommand(gen),
		dispatcher.SubscribeCommand(cleanup),
		dispatcher.SubscribeQuery(status),
		dispatcher.SubscribeQuery(history),
		dispatcher.SubscribeQuery(download),
	}

	if reg != nil {
		handlers := []any{
			req,
			cancel,
			del,
			gen,
			cleanup,
			status,
			history,
			download,
		}
		for _, handler := range handlers {
			if err := reg.RegisterCommand(handler); err != nil {
				return subscriptions, err
			}
		}
	}

	return subscriptions, nil
}
