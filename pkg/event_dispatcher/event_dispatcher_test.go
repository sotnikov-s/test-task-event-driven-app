package event_dispatcher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sotnikov-s/test-task-event-driven-app/pkg/action"
	"github.com/sotnikov-s/test-task-event-driven-app/pkg/event"
)

func TestAddAndRemoveAction(t *testing.T) {
	disp := buildDispatcher(t)

	assert.Equalf(t, 2, len(disp.actions), "add of two actions expected to result in the actions slice len of 2")
	assert.Equal(t, actionStraightCheckName, disp.actions[0].Name)
	assert.Equal(t, actionDividerName, disp.actions[1].Name)

	if t.Failed() {
		return
	}
	if err := disp.RemoveAction(actionDivider); err != nil {
		t.Fatalf("remove of %s action shouldn't returned an error: %v", actionDivider.Name, err)
	}

	assert.Equalf(t, 1, len(disp.actions), "add of two actions followed by remove of one expected to result in the actions slice len of 1")
	assert.Equal(t, actionStraightCheckName, disp.actions[0].Name)

	assert.Errorf(t, disp.AddAction(actionStraightCheck), "add of an already existing should return an error")
	assert.Errorf(t, disp.RemoveAction(actionDivider), "remove of an nonexistent action should return an error")
}

func TestHandleSingleEvent(t *testing.T) {
	disp := buildDispatcher(t)
	ev := event.Event{"dividend": 10, "divider": 5}

	jobId, err := disp.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("handle of event shouldn't returned an error: %v", err)
	}

	expectedJobId := 0
	assert.Equalf(t, expectedJobId, jobId, "the first event handling should return 0 as the jobId")
	assert.Equalf(t, jobId, disp.snapshots[expectedJobId].Id, "the returned jobId should be equal to the one saved in the snapshot")
	assert.Truef(t, disp.snapshots[expectedJobId].Success, "a succeeded job snapshot should have Success=true")
	assert.Equalf(t, ev, disp.snapshots[expectedJobId].Event, "the passed and the stored in the snapshot events mismatch")
	assert.Equalf(t, 2, len(disp.snapshots[expectedJobId].ActionsResults), "the succeeded job snapshot should have two action results")
	assert.Equalf(t, 0, len(disp.snapshots[expectedJobId].actionsToProcess), "a succeeded job snapshot shouldn't have any actions to process")
}

func TestHandleEventWithFail(t *testing.T) {
	disp := buildDispatcher(t)
	ev := event.Event{"dividend": 10, "divider": 0}

	jobId, err := disp.HandleEvent(context.Background(), ev)
	if err == nil {
		t.Fatalf("handle of a meant-to-fail event should returned an error")
	}

	expectedJobId := 0
	assert.Equalf(t, expectedJobId, jobId, "the first event handling should return 0 as the jobId")
	assert.Equalf(t, jobId, disp.snapshots[expectedJobId].Id, "the returned jobId should be equal to the one saved in the snapshot")
	assert.Falsef(t, disp.snapshots[expectedJobId].Success, "a failed job snapshot should have Success=false")
	assert.Equalf(t, ev, disp.snapshots[expectedJobId].Event, "the passed and the stored in the snapshot events mismatch")

	assert.Equalf(t, 1, len(disp.snapshots[expectedJobId].ActionsResults), "the succeeded job snapshot should have one action result")
	assert.Equalf(t, actionStraightCheckName, disp.snapshots[expectedJobId].ActionsResults[0].Name, "the action result expected to be %s", actionStraightCheckName)

	assert.Equalf(t, 1, len(disp.snapshots[expectedJobId].actionsToProcess), "the succeeded job snapshot should have one action to process")
	assert.Equalf(t, actionDividerName, disp.snapshots[expectedJobId].actionsToProcess[0].Name, "the remaining to be processed action should be the %s", actionDividerName)
}

func TestRetryFailedJob(t *testing.T) {
	disp := buildDispatcher(t)
	ev := event.Event{"dividend": 10, "divider": 5}

	_, err := disp.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("handle of event shouldn't returned an error: %v", err)
	}

	setStraightCheckErr(fmt.Errorf("an error"))
	expectedJobId := 1
	jobId, err := disp.HandleEvent(context.Background(), ev)
	if err == nil {
		t.Fatalf("handle of a meant-to-fail event should returned an error")
	}
	if jobId != expectedJobId {
		t.Fatalf("the meant-to-fail handling should returned 1 as jobId")
	}

	setStraightCheckErr(nil)
	err = disp.RetryJob(jobId)
	if err != nil {
		t.Fatalf("the fixed job should returned no error on retry: %v", err)
	}

	assert.Truef(t, disp.snapshots[expectedJobId].Success, "a succeeded job snapshot should have Success=true")
	assert.Equalf(t, 2, len(disp.snapshots[expectedJobId].ActionsResults), "the fixed job snapshot should have two action results after retry")
	assert.Equalf(t, 0, len(disp.snapshots[expectedJobId].actionsToProcess), "the fixed job snapshot shouldn't have any actions to process after retry")
}

func TestSnapshotActionsImmutability(t *testing.T) {
	disp := buildDispatcher(t)
	ev := event.Event{"dividend": 10, "divider": 5}

	_, err := disp.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("handle of event shouldn't returned an error: %v", err)
	}

	setStraightCheckErr(fmt.Errorf("an error"))
	expectedJobId := 1
	jobId, err := disp.HandleEvent(context.Background(), ev)
	if err == nil {
		t.Fatalf("handle of a meant-to-fail event should returned an error")
	}
	if jobId != expectedJobId {
		t.Fatalf("the meant-to-fail handling should returned 1 as jobId")
	}

	err = disp.AddAction(actionEnpty)
	if err != nil {
		t.Fatalf("add of %s shouldn't return an error: %v", actionEnpty.Name, err)
	}
	setStraightCheckErr(nil)
	err = disp.RetryJob(jobId)
	if err != nil {
		t.Fatalf("the fixed job should returned no error on retry: %v", err)
	}

	assert.Truef(t, disp.snapshots[expectedJobId].Success, "a succeeded job snapshot should have Success=true")
	assert.Equalf(t, 3, len(disp.actions), "by the end of the test the desp should have three set actions")
	assert.Equalf(t, 2, len(disp.snapshots[expectedJobId].ActionsResults), "the fixed job snapshot should have two action results after retry")
	assert.Equalf(t, 0, len(disp.snapshots[expectedJobId].actionsToProcess), "the fixed job snapshot shouldn't have any actions to process after retry")
}

func TestRetrySucceededJob(t *testing.T) {
	disp := buildDispatcher(t)
	ev := event.Event{"dividend": 10, "divider": 5}

	jobId, err := disp.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("handle of event shouldn't returned an error: %v", err)
	}

	assert.Error(t, disp.RetryJob(jobId), "retry of a successfully completed job should return an error")
}

func TestRetryInvalidJob(t *testing.T) {
	disp := buildDispatcher(t)
	ev := event.Event{"dividend": 10, "divider": 5}

	_, err := disp.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("handle of event shouldn't returned an error: %v", err)
	}

	assert.Error(t, disp.RetryJob(-1), "retry call with a negative jobId should return an error")
	assert.Error(t, disp.RetryJob(1), "retry call for a nonexistent jobId should return an error")
	assert.Error(t, disp.RetryJob(10), "retry call for a nonexistent jobId should return an error")
}

func TestNilEventPassing(t *testing.T) {
	disp := buildDispatcher(t)

	_, err := disp.HandleEvent(context.Background(), nil)
	if err == nil {
		t.Fatalf("handle of event should returned an error")
	}
}

func buildDispatcher(t *testing.T) *EventDispatcher {
	disp := NewEventDispatcher()
	if err := disp.AddAction(actionStraightCheck); err != nil {
		t.Fatalf("add %s action shouldn't returned an error: %v", actionStraightCheck.Name, err)
	}
	if err := disp.AddAction(actionDivider); err != nil {
		t.Fatalf("add %s action shouldn't returned an error: %v", actionDivider.Name, err)
	}
	return disp
}

var (
	actionStraightCheck = action.Action{
		Name: actionStraightCheckName,
		Do: func(ctx context.Context, input map[string]interface{}) (output map[string]interface{}, err error) {
			return nil, straightCheckErr
		},
	}
	straightCheckErr    error
	setStraightCheckErr = func(err error) {
		straightCheckErr = err
	}
)

var actionDivider = action.Action{
	Name: actionDividerName,
	Do: func(ctx context.Context, input map[string]interface{}) (output map[string]interface{}, err error) {
		dividend, ex := input["dividend"]
		if !ex {
			return nil, fmt.Errorf("no dividend passed")
		}
		intDividend, ok := dividend.(int)
		if !ok {
			return nil, fmt.Errorf("the passed dividend expected to be of type int")
		}

		divider, ex := input["divider"]
		if !ex {
			return nil, fmt.Errorf("no divider passed")
		}
		intDivider, ok := divider.(int)
		if !ok {
			return nil, fmt.Errorf("the passed divider expected to be of type int")
		}
		if intDivider == 0 {
			return nil, fmt.Errorf("the passed divider shouldn't be equal to zero")
		}

		output = make(map[string]interface{})
		output["result"] = intDividend / intDivider
		return output, nil
	},
}

var actionEnpty = action.Action{
	Name: actionEmptyName,
	Do: func(ctx context.Context, input map[string]interface{}) (output map[string]interface{}, err error) {
		return nil, nil
	},
}

const actionDividerName = "divider"
const actionStraightCheckName = "straight_check"
const actionEmptyName = "empty_action"
