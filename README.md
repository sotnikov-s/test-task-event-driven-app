Consider we have an event driven system and there is a function that executes a list of another functions which are called actions. If one of the functions returned an error then execution breaks. We need to build a snapshot feature to save incoming event, input/output, execution state and timestamp for each action. And as the last point that feature should have a function to restore saved state and execute actions which were not successfully completed or started.

Models and signatures:
The event: map[string]interface{},
The first function: func(ctx context.Context, event map[string]interface{}) error.
The action: func (ctx context.Context, input map[string]interface{})(map[string]interface{}, error).

Note:
- the input of each action is builded by some mapper, but you should skip that and just define a map of inputs somewhere and use it.
- The snapshot should save the state of execution for each incoming event, so the function for state restoring must accept id (int) to find a single state.
