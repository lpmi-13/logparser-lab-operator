package notifier

import (
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// Event represents a single notification delivered to SSE clients.
type Event struct {
	Message            string    `json:"message"`
	Kind               string    `json:"kind,omitempty"`
	ChallengeID        string    `json:"challengeId,omitempty"`
	ActivityID         string    `json:"activityId,omitempty"`
	InstructionSummary string    `json:"instructionSummary,omitempty"`
	Lab                string    `json:"lab,omitempty"`
	SentAt             time.Time `json:"sentAt"`
}

// Notifier handles sending notifications to SSE clients.
type Notifier struct {
	mu                 sync.RWMutex
	subscribers        map[int]chan Event
	nextID             int
	lastNotified       map[string]*notificationState
	lastChangeNotified map[string]time.Time
	pendingTimers      map[string]*time.Timer
}

type notificationState struct {
	lastEvent Event
	lastTime  time.Time
	pending   *Event
}

// New creates a new Notifier instance.
func New() *Notifier {
	return &Notifier{
		subscribers:        make(map[int]chan Event),
		lastNotified:       make(map[string]*notificationState),
		lastChangeNotified: make(map[string]time.Time),
		pendingTimers:      make(map[string]*time.Timer),
	}
}

// SendEvent sends a structured notification with a short debounce window.
func (n *Notifier) SendEvent(labName string, event Event) {
	if n == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	event = normalizeEvent(labName, event, now)
	state, exists := n.lastNotified[labName]

	if exists && sameLogicalEvent(state.lastEvent, event) {
		return
	}

	if exists && time.Since(state.lastTime) < 2*time.Second {
		if timer, hasTimer := n.pendingTimers[labName]; hasTimer {
			timer.Stop()
		}

		pending := event
		state.pending = &pending
		remaining := 2*time.Second - time.Since(state.lastTime)
		n.pendingTimers[labName] = time.AfterFunc(remaining, func() {
			n.deliverPending(labName)
		})
		return
	}

	n.lastNotified[labName] = &notificationState{
		lastEvent: event,
		lastTime:  now,
	}
	delete(n.lastChangeNotified, labName)
	n.fanOut(event)
}

func (n *Notifier) deliverPending(labName string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	state, exists := n.lastNotified[labName]
	if !exists || state.pending == nil {
		return
	}

	event := *state.pending
	state.pending = nil
	state.lastEvent = event
	state.lastTime = time.Now()

	delete(n.pendingTimers, labName)
	delete(n.lastChangeNotified, labName)

	n.fanOut(event)
}

// SendChangeEvent sends a structured notification with a 30 second cooldown.
func (n *Notifier) SendChangeEvent(labName string, event Event) {
	if n == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	event = normalizeEvent(labName, event, time.Now())
	if time.Since(n.lastChangeNotified[labName]) <= 30*time.Second {
		return
	}

	n.lastChangeNotified[labName] = time.Now()
	n.fanOut(event)
}

// Subscribe registers a new SSE client and returns its channel plus cleanup callback.
func (n *Notifier) Subscribe() (<-chan Event, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()

	id := n.nextID
	n.nextID++

	ch := make(chan Event, 10)
	n.subscribers[id] = ch

	for _, state := range n.lastNotified {
		if state.lastEvent.Message == "" {
			continue
		}
		select {
		case ch <- state.lastEvent:
		default:
			ctrl.Log.WithName("notifier").Info("Could not send initial state to new client")
		}
		break
	}

	cleanup := func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.subscribers, id)
		close(ch)
	}

	return ch, cleanup
}

func (n *Notifier) fanOut(event Event) {
	for _, ch := range n.subscribers {
		select {
		case ch <- event:
		default:
			ctrl.Log.WithName("notifier").Info("Skipped notification to slow client")
		}
	}
}

func normalizeEvent(labName string, event Event, now time.Time) Event {
	if event.Lab == "" {
		event.Lab = labName
	}
	if event.SentAt.IsZero() {
		event.SentAt = now
	}
	return event
}

func sameLogicalEvent(a, b Event) bool {
	return a.Message == b.Message && a.ChallengeID == b.ChallengeID
}
