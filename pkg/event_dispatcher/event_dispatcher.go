package event_dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sotnikov-s/test-task-event-driven-app/pkg/action"
	"github.com/sotnikov-s/test-task-event-driven-app/pkg/event"
)

// EventDispatcher is a concurrently save runtime changeable actions list that sends incoming events
// to the actions and keeps the execution context, state and result as snapshots.
type EventDispatcher struct {
	mu           *sync.Mutex
	actions      []action.Action
	currentJobId int
	snapshots    map[int]*snapshot
}

// NewEventDispatcher is the EventDispatcher constructor.
func NewEventDispatcher() *EventDispatcher {
	return &EventDispatcher{
		mu:           &sync.Mutex{},
		actions:      make([]action.Action, 0),
		currentJobId: 0,
		snapshots:    make(map[int]*snapshot),
	}
}

// AddAction adds an action to the processor actions list which from now on will be called for
// handling incoming events. It returns an error if the dispatcher's actions list already contains
// an action with the same name as in the act parameter.
func (d *EventDispatcher) AddAction(act action.Action) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, action := range d.actions {
		if act.Name == action.Name {
			return fmt.Errorf("action with name %s already exists", act.Name)
		}
	}
	d.actions = append(d.actions, act)
	return nil
}

// RemoveAction removes an action from the dispatcher's actions list so it won't be called for
// further events. It returns an error if the actions list doesn't contain an action with the name
// specified in the act param.
func (d *EventDispatcher) RemoveAction(act action.Action) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for idx, action := range d.actions {
		if act.Name == action.Name {
			d.actions = append(d.actions[:idx], d.actions[idx+1:]...)
			return nil
		}
	}
	return fmt.Errorf("action %s doesn't exist", act.Name)
}

// HandleEvent executes all current actions from the actions list for the passed event and preserves
// the event handling snapshot which will have the returned int as jobId. If any of the called actions
// fails, the handling returns an error.
func (d *EventDispatcher) HandleEvent(ctx context.Context, event event.Event) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	actions := append(d.actions[:0:0], d.actions...)
	jobId := d.currentJobId
	d.currentJobId++

	return jobId, d.handleEventByActions(ctx, event, actions, jobId)
}

// RetryJob loads a snapshot with the specified jobId and tries to finish the event handling by
// executing the failed and the not yet called actions specified by the time when the event was
// being processed for the first time. It returns an error if any action fails or if the jobId
// is invalid or if the job with the specified id already has been successfully processed.
func (d *EventDispatcher) RetryJob(jobId int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.validateRetryJobId(jobId); err != nil {
		return err
	}

	storedSnapshot := d.snapshots[jobId]
	return d.handleEventByActions(storedSnapshot.Context, storedSnapshot.Event, storedSnapshot.actionsToProcess, jobId)
}

// handleEventByActions executes the specified actions list for the passed event and then preserves the
// snapshot with the passed jobId. It returns an error if any action fails.
func (d *EventDispatcher) handleEventByActions(ctx context.Context, event event.Event, actions []action.Action, jobId int) error {
	result := &snapshot{
		Id:               jobId,
		Event:            event,
		Context:          ctx,
		Success:          false,
		actionsToProcess: actions,
	}
	defer func() {
		d.snapshots[jobId] = result
	}()

	for ; len(result.actionsToProcess) > 0; result.actionsToProcess = result.actionsToProcess[1:] {
		action := result.actionsToProcess[0]
		output, err := action.Do(ctx, event)
		if err != nil {
			return fmt.Errorf("%s action failed: %v", action.Name, err)
		}
		result.ActionsResults = append(result.ActionsResults, actionResult{
			Name:      action.Name,
			Input:     event,
			Output:    output,
			Timestamp: time.Now().String(),
		})
	}

	result.Success = true
	return nil
}

// validateRetryJobId checks whether the passed jobId has valid value and the stored snapshot
// has really failed.
func (d *EventDispatcher) validateRetryJobId(jobId int) error {
	if jobId >= len(d.snapshots) || jobId < 0 {
		return ErrorInvalidJobId
	}
	if d.snapshots[jobId].Success {
		return ErrorSucceededJobRetryCall
	}

	return nil
}

// snapshot is a set of parameters both incoming and result important for an event handling process.
type snapshot struct {
	Id             int
	Event          event.Event
	ActionsResults []actionResult
	Context        context.Context
	Success        bool

	actionsToProcess []action.Action
}

// actionResult stores a successfully executed action context and result.
type actionResult struct {
	Name      string
	Input     map[string]interface{}
	Output    map[string]interface{}
	Timestamp string
}

var ErrorInvalidJobId = errors.New("invalid job id")
var ErrorSucceededJobRetryCall = errors.New("retry called for a successfully completed job")
