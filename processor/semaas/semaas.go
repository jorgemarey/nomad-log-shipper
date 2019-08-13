package semaas

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jorgemarey/nomad-log-shipper/processor"
	"globaldevtools.bbva.com/entsec/semaas.git/client/omega"
	"globaldevtools.bbva.com/entsec/semaas.git/client/rho"
)

type semaasProcessor struct {
	level omega.LogLevel
}

func NewSemaasProcessor() processor.Processor {
	return &semaasProcessor{
		level: omega.LogLevelInfo,
	}
}

func (p *semaasProcessor) Process(text string, properties map[string]interface{}, meta map[string]string) (string, []byte, error) {
	log, span, ok := p.formatted(text, properties)
	if !ok {
		log = &omega.LogEntry{
			Message:      text,
			CreationDate: time.Now(),
			Level:        p.level,
			Properties:   properties,
		}
	}

	if log != nil {
		if log.MrID == "" {
			log.MrID = meta["mrid"]
		}
		data := &entryData{
			Meta:     meta,
			LogEntry: log,
		}
		db, err := json.Marshal(data)
		if err != nil {
			return "", nil, fmt.Errorf("Error marshaling data: %s", err)
		}
		return "log", db, nil
	}

	if span != nil {
		if span.MrID == "" {
			span.MrID = meta["mrid"]
		}
		data := &spanData{
			Meta: meta,
			Span: span,
		}
		db, err := json.Marshal(data)
		if err != nil {
			return "", nil, fmt.Errorf("Error marshaling data: %s", err)
		}
		return "span", db, nil
	}

	return "", nil, fmt.Errorf("No data found")
}

func (p *semaasProcessor) formatted(text string, properties map[string]interface{}) (*omega.LogEntry, *rho.Span, bool) {
	if !strings.HasPrefix(text, "V2|") {
		return nil, nil, false
	}
	parts := strings.Split(text, "|")
	if len(parts) < 3 {
		return nil, nil, false
	}
	kind := parts[1]
	data := strings.Join(parts[2:], "|")

	switch {
	case strings.HasPrefix(kind, "LOG"): // it is a log
		var entry omega.LogEntry
		if err := json.NewDecoder(strings.NewReader(data)).Decode(&entry); err != nil {
			return nil, nil, false
		}
		// set the level
		if entry.Level == "" {
			levelParts := strings.Split(kind, ".")
			entry.Level = p.level
			if len(levelParts) > 1 {
				entry.Level = omega.LogLevel(levelParts[1])
			}
		}
		// we add the custom properties here
		if entry.Properties == nil {
			entry.Properties = make(map[string]interface{})
		}
		for k, v := range properties {
			entry.Properties[k] = v
		}
		return &entry, nil, true
	case strings.HasPrefix(kind, "SPAN"): // it is a span
		var span rho.Span
		if err := json.NewDecoder(strings.NewReader(data)).Decode(&span); err != nil {
			return nil, nil, false
		}
		// we add the custom properties here
		if span.Properties == nil {
			span.Properties = make(map[string]interface{})
		}
		for k, v := range properties {
			span.Properties[k] = v
		}
		return nil, &span, true

	default:
		return nil, nil, false
	}
}

type entryData struct {
	Meta map[string]string `json:"meta"`
	*omega.LogEntry
}

func (d *entryData) MarshalJSON() ([]byte, error) {
	p, err := json.Marshal(d.LogEntry)
	if err != nil {
		return nil, err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(p, &data); err != nil {
		return nil, err
	}

	data["meta"] = d.Meta
	return json.Marshal(data)
}

type spanData struct {
	Meta map[string]string `json:"meta"`
	*rho.Span
}

func (d *spanData) MarshalJSON() ([]byte, error) {
	p, err := json.Marshal(d.Span)
	if err != nil {
		return nil, err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(p, &data); err != nil {
		return nil, err
	}

	data["meta"] = d.Meta
	return json.Marshal(data)
}
