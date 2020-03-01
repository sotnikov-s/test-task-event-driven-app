package event

// Event is a key-value representation of a happening in a system. It's being transfered between
// the system services to communicate and provide required data for handling.
type Event map[string]interface{}
