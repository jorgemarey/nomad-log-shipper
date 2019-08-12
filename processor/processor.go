package processor

// Processor defines an interface for any kind of processor
type Processor interface {
	Process(text string, properties map[string]interface{}, meta map[string]string) (string, []byte, error)
}
