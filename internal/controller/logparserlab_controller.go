package controller

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	labv1alpha1 "github.com/lpmi-13/logparser-lab-operator/api/v1alpha1"
	"github.com/lpmi-13/logparser-lab-operator/internal/challenges"
	"github.com/lpmi-13/logparser-lab-operator/internal/notifier"
)

// LogParserLabReconciler reconciles a LogParserLab object.
type LogParserLabReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Notifier   *notifier.Notifier
	LogsDir    string
	AnswerRoot string

	mu              sync.Mutex
	rng             *rand.Rand
	lastSubmission  map[string]string
	activeChallenge map[string]string
}

// +kubebuilder:rbac:groups=lab.learning.io,resources=logparserlabs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=lab.learning.io,resources=logparserlabs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=lab.learning.io,resources=logparserlabs/finalizers,verbs=update

func (r *LogParserLabReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling LogParserLab", "name", req.Name, "namespace", req.Namespace)

	var lab labv1alpha1.LogParserLab
	if err := r.Get(ctx, req.NamespacedName, &lab); err != nil {
		if errors.IsNotFound(err) {
			r.clearTracking(fmt.Sprintf("%s/%s", req.Namespace, req.Name))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	switch lab.Status.State {
	case "", labv1alpha1.StateInitialized:
		return r.initializeLab(ctx, &lab)
	case labv1alpha1.StateActive:
		return r.checkSubmission(ctx, &lab)
	case labv1alpha1.StateCompleted:
		return r.resetLab(ctx, &lab)
	case labv1alpha1.StateError:
		return ctrl.Result{}, nil
	default:
		if err := r.updateStatusWithRetry(ctx, &lab, func(l *labv1alpha1.LogParserLab) {
			l.Status.State = labv1alpha1.StateInitialized
			l.Status.Message = "Resetting unknown state back to Initialized."
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
}

func (r *LogParserLabReconciler) initializeLab(ctx context.Context, lab *labv1alpha1.LogParserLab) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	labKey := r.labKey(lab)
	answerPath := r.answerPathForLab(lab)
	logsDir, err := r.logsDirForLab(lab)
	if err != nil {
		return r.failLab(ctx, lab, err)
	}

	r.sendSetupEvent(labKey, "Preparing the local log parsing workspace...")

	activity, completed, err := r.selectActivity(lab.Status.CompletedActivities, lab.Spec.Activity)
	if err != nil {
		return r.failLab(ctx, lab, err)
	}

	roundSeed := r.nextSeed()
	scenario, err := challenges.Prepare(activity, roundSeed)
	if err != nil {
		return r.failLab(ctx, lab, err)
	}

	if err := r.cleanupLogWorkspace(logsDir); err != nil {
		return r.failLab(ctx, lab, fmt.Errorf("clean log workspace: %w", err))
	}

	currentLogPath, err := scenario.WriteLog(logsDir)
	if err != nil {
		return r.failLab(ctx, lab, fmt.Errorf("write scenario log: %w", err))
	}

	if err := r.resetAnswerFile(answerPath); err != nil {
		return r.failLab(ctx, lab, fmt.Errorf("reset answer file: %w", err))
	}

	nextRound := lab.Status.Round + 1
	challengeText := r.renderChallengeText(answerPath, currentLogPath, scenario)

	if err := r.updateStatusWithRetry(ctx, lab, func(l *labv1alpha1.LogParserLab) {
		l.Status.CurrentActivityID = activity.ID
		l.Status.CurrentTitle = scenario.Title
		l.Status.CurrentDataset = scenario.LogName
		l.Status.Question = scenario.Question
		l.Status.OutputFormat = scenario.OutputFormat
		l.Status.SuggestedTools = append([]string(nil), scenario.SuggestedTools...)
		l.Status.State = labv1alpha1.StateActive
		l.Status.Message = "Challenge ready. Submit the answer by writing to the answer file."
		l.Status.LogsDir = logsDir
		l.Status.CurrentLogPath = currentLogPath
		l.Status.AnswerFile = answerPath
		l.Status.CompletedActivities = completed
		l.Status.Round = nextRound
		l.Status.RoundSeed = roundSeed
	}); err != nil {
		return ctrl.Result{}, err
	}

	r.setActiveChallenge(labKey, fmt.Sprintf("%s-%d", lab.Name, nextRound))
	r.clearLastSubmission(labKey)

	logger.Info("Challenge activated", "activity", activity.ID, "round", nextRound, "lab", labKey)
	r.sendScenarioEvent(labKey, "ready", challengeText, scenario)

	return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *LogParserLabReconciler) checkSubmission(ctx context.Context, lab *labv1alpha1.LogParserLab) (ctrl.Result, error) {
	labKey := r.labKey(lab)
	if lab.Status.RoundSeed == 0 {
		if err := r.updateStatusWithRetry(ctx, lab, func(l *labv1alpha1.LogParserLab) {
			l.Status.CurrentActivityID = ""
			l.Status.CurrentTitle = ""
			l.Status.CurrentDataset = ""
			l.Status.Question = ""
			l.Status.OutputFormat = ""
			l.Status.SuggestedTools = nil
			l.Status.CurrentLogPath = ""
			l.Status.State = labv1alpha1.StateInitialized
			l.Status.Message = "Regenerating challenge with dynamic logs."
			l.Status.RoundSeed = 0
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	activity, ok := challenges.Lookup(lab.Status.CurrentActivityID)
	if !ok {
		return r.failLab(ctx, lab, fmt.Errorf("activity %q not found", lab.Status.CurrentActivityID))
	}
	scenario, _, err := r.ensureActiveScenario(lab, activity)
	if err != nil {
		return r.failLab(ctx, lab, err)
	}

	answerPath := lab.Status.AnswerFile
	if answerPath == "" {
		answerPath = r.answerPathForLab(lab)
	}

	answer, found, err := r.readAnswer(answerPath)
	if err != nil {
		return r.failLab(ctx, lab, fmt.Errorf("read answer file: %w", err))
	}
	if !found || answer == "" {
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	if answer == challenges.NormalizeAnswer(scenario.ExpectedAnswer) {
		if err := r.updateStatusWithRetry(ctx, lab, func(l *labv1alpha1.LogParserLab) {
			l.Status.State = labv1alpha1.StateCompleted
			l.Status.Message = "Correct answer received. Resetting for the next activity."
		}); err != nil {
			return ctrl.Result{}, err
		}
		logsDir, err := r.activeLogsDir(lab)
		if err != nil {
			return r.failLab(ctx, lab, err)
		}
		if err := r.cleanupLogWorkspace(logsDir); err != nil {
			return r.failLab(ctx, lab, fmt.Errorf("clean log workspace: %w", err))
		}

		r.sendScenarioEvent(labKey, "completed", r.renderCompletedText(), scenario)
		return ctrl.Result{Requeue: true}, nil
	}

	if answer != r.lastSubmitted(labKey) {
		r.setLastSubmission(labKey, answer)
		r.sendScenarioChangeEvent(labKey, "attempt", r.renderIncorrectAnswerText(answerPath), scenario)
	}

	return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *LogParserLabReconciler) resetLab(ctx context.Context, lab *labv1alpha1.LogParserLab) (ctrl.Result, error) {
	labKey := r.labKey(lab)
	answerPath := lab.Status.AnswerFile
	if answerPath == "" {
		answerPath = r.answerPathForLab(lab)
	}

	if scenario, ok := r.scenarioForStatus(lab); ok {
		r.sendScenarioEvent(labKey, "reset", "Resetting the answer file and preparing the next activity...", scenario)
	} else {
		r.sendEvent(labKey, "reset", "Resetting the answer file and preparing the next activity...", lab.Status.CurrentActivityID)
	}

	logsDir, err := r.activeLogsDir(lab)
	if err != nil {
		return r.failLab(ctx, lab, err)
	}
	if err := r.cleanupLogWorkspace(logsDir); err != nil {
		return r.failLab(ctx, lab, fmt.Errorf("clean log workspace: %w", err))
	}

	if err := r.resetAnswerFile(answerPath); err != nil {
		return r.failLab(ctx, lab, fmt.Errorf("reset answer file: %w", err))
	}

	if err := r.updateStatusWithRetry(ctx, lab, func(l *labv1alpha1.LogParserLab) {
		l.Status.CurrentActivityID = ""
		l.Status.CurrentTitle = ""
		l.Status.CurrentDataset = ""
		l.Status.Question = ""
		l.Status.OutputFormat = ""
		l.Status.SuggestedTools = nil
		l.Status.State = labv1alpha1.StateInitialized
		l.Status.Message = "Preparing a new challenge."
		l.Status.AnswerFile = answerPath
		l.Status.CurrentLogPath = ""
		l.Status.RoundSeed = 0
	}); err != nil {
		return ctrl.Result{}, err
	}

	r.clearLastSubmission(labKey)
	r.clearActiveChallenge(labKey)

	return ctrl.Result{Requeue: true}, nil
}

func (r *LogParserLabReconciler) failLab(ctx context.Context, lab *labv1alpha1.LogParserLab, err error) (ctrl.Result, error) {
	log.FromContext(ctx).Error(err, "marking lab as failed", "name", lab.Name)
	updateErr := r.updateStatusWithRetry(ctx, lab, func(l *labv1alpha1.LogParserLab) {
		l.Status.State = labv1alpha1.StateError
		l.Status.Message = err.Error()
	})
	if updateErr != nil {
		return ctrl.Result{}, updateErr
	}
	if scenario, ok := r.scenarioForStatus(lab); ok {
		r.sendScenarioEvent(r.labKey(lab), "error", fmt.Sprintf("Lab entered an error state:\n%s", err.Error()), scenario)
	} else {
		r.sendEvent(r.labKey(lab), "error", fmt.Sprintf("Lab entered an error state:\n%s", err.Error()), lab.Status.CurrentActivityID)
	}
	return ctrl.Result{}, err
}

func (r *LogParserLabReconciler) renderChallengeText(answerPath, currentLogPath string, scenario challenges.Scenario) string {
	return strings.Join([]string{
		fmt.Sprintf("Log file: %s", currentLogPath),
		fmt.Sprintf("Answer file: %s", answerPath),
		"",
		"Run your pipeline directly on the VM filesystem and redirect stdout to the answer file:",
		"```sh",
		fmt.Sprintf("cat %s | <your pipeline> > %s", currentLogPath, answerPath),
		"```",
	}, "\n")
}

func (r *LogParserLabReconciler) renderIncorrectAnswerText(answerPath string) string {
	return fmt.Sprintf("Checked %s, but the output is not correct yet.", answerPath)
}

func (r *LogParserLabReconciler) renderCompletedText() string {
	return "Correct.\nResetting the answer file and selecting the next activity."
}

func (r *LogParserLabReconciler) ensureActiveScenario(lab *labv1alpha1.LogParserLab, activity challenges.Activity) (challenges.Scenario, string, error) {
	scenario, err := challenges.Prepare(activity, lab.Status.RoundSeed)
	if err != nil {
		return challenges.Scenario{}, "", err
	}

	logsDir, err := r.activeLogsDir(lab)
	if err != nil {
		return challenges.Scenario{}, "", err
	}
	logPath := lab.Status.CurrentLogPath
	if logPath == "" {
		logPath = filepath.Join(logsDir, scenario.LogName)
	}

	info, err := os.Stat(logPath)
	if err == nil {
		if info.IsDir() {
			return challenges.Scenario{}, "", fmt.Errorf("log path %s is a directory, expected a file", logPath)
		}
		return scenario, logPath, nil
	}
	if !os.IsNotExist(err) {
		return challenges.Scenario{}, "", err
	}

	if err := r.cleanupLogWorkspace(logsDir); err != nil {
		return challenges.Scenario{}, "", err
	}
	logPath, err = scenario.WriteLog(logsDir)
	if err != nil {
		return challenges.Scenario{}, "", err
	}
	return scenario, logPath, nil
}

func (r *LogParserLabReconciler) scenarioForStatus(lab *labv1alpha1.LogParserLab) (challenges.Scenario, bool) {
	if lab.Status.CurrentActivityID == "" || lab.Status.RoundSeed == 0 {
		return challenges.Scenario{}, false
	}
	activity, ok := challenges.Lookup(lab.Status.CurrentActivityID)
	if !ok {
		return challenges.Scenario{}, false
	}
	scenario, err := challenges.Prepare(activity, lab.Status.RoundSeed)
	if err != nil {
		return challenges.Scenario{}, false
	}
	return scenario, true
}

func (r *LogParserLabReconciler) answerPathForLab(lab *labv1alpha1.LogParserLab) string {
	switch {
	case lab.Spec.AnswerFile == "":
		return filepath.Join(r.AnswerRoot, lab.Name, "answer.txt")
	case filepath.IsAbs(lab.Spec.AnswerFile):
		return lab.Spec.AnswerFile
	default:
		return filepath.Join(r.AnswerRoot, lab.Spec.AnswerFile)
	}
}

func (r *LogParserLabReconciler) logsDirForLab(lab *labv1alpha1.LogParserLab) (string, error) {
	base := r.LogsDir
	if lab.Spec.LogsDir != "" {
		base = lab.Spec.LogsDir
	}
	if base == "" {
		base = "./logs"
	}
	return filepath.Abs(base)
}

func (r *LogParserLabReconciler) activeLogsDir(lab *labv1alpha1.LogParserLab) (string, error) {
	if lab.Status.LogsDir != "" {
		return lab.Status.LogsDir, nil
	}
	return r.logsDirForLab(lab)
}

func (r *LogParserLabReconciler) cleanupLogWorkspace(logsDir string) error {
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		if err := os.Remove(filepath.Join(logsDir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (r *LogParserLabReconciler) resetAnswerFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte{}, 0o644)
}

func (r *LogParserLabReconciler) readAnswer(path string) (string, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return challenges.NormalizeAnswer(string(raw)), true, nil
}

func (r *LogParserLabReconciler) labKey(lab *labv1alpha1.LogParserLab) string {
	return fmt.Sprintf("%s/%s", lab.Namespace, lab.Name)
}

func (r *LogParserLabReconciler) updateStatusWithRetry(ctx context.Context, lab *labv1alpha1.LogParserLab, updateFn func(*labv1alpha1.LogParserLab)) error {
	return wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
		Steps:    5,
		Duration: 100 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.1,
	}, func(ctx context.Context) (bool, error) {
		key := client.ObjectKeyFromObject(lab)
		if err := r.Get(ctx, key, lab); err != nil {
			return false, err
		}

		updateFn(lab)
		if err := r.Status().Update(ctx, lab); err != nil {
			if errors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}

func (r *LogParserLabReconciler) sendSetupEvent(labKey, message string) {
	if r.Notifier == nil {
		return
	}
	r.Notifier.SendEvent(labKey, notifier.Event{
		Message:     message,
		Kind:        "setup",
		ChallengeID: r.challengeID(labKey),
		Lab:         labKey,
	})
}

func (r *LogParserLabReconciler) sendEvent(labKey, kind, message, activityID string) {
	if r.Notifier == nil {
		return
	}
	r.Notifier.SendEvent(labKey, notifier.Event{
		Message:     message,
		Kind:        kind,
		ChallengeID: r.challengeID(labKey),
		ActivityID:  activityID,
		Lab:         labKey,
	})
}

func (r *LogParserLabReconciler) sendScenarioEvent(labKey, kind, message string, scenario challenges.Scenario) {
	if r.Notifier == nil {
		return
	}
	r.Notifier.SendEvent(labKey, notifier.Event{
		Message:            message,
		Kind:               kind,
		ChallengeID:        r.challengeID(labKey),
		ActivityID:         scenario.ActivityID,
		InstructionSummary: r.instructionSummaryForEvent(kind, scenario),
		Lab:                labKey,
	})
}

func (r *LogParserLabReconciler) sendChangeEvent(labKey, kind, message, activityID string) {
	if r.Notifier == nil {
		return
	}
	r.Notifier.SendChangeEvent(labKey, notifier.Event{
		Message:     message,
		Kind:        kind,
		ChallengeID: r.challengeID(labKey),
		ActivityID:  activityID,
		Lab:         labKey,
	})
}

func (r *LogParserLabReconciler) instructionSummaryForEvent(kind string, scenario challenges.Scenario) string {
	if kind == "completed" {
		return ""
	}
	return scenario.InstructionSummary()
}

func (r *LogParserLabReconciler) sendScenarioChangeEvent(labKey, kind, message string, scenario challenges.Scenario) {
	if r.Notifier == nil {
		return
	}
	r.Notifier.SendChangeEvent(labKey, notifier.Event{
		Message:            message,
		Kind:               kind,
		ChallengeID:        r.challengeID(labKey),
		ActivityID:         scenario.ActivityID,
		InstructionSummary: scenario.InstructionSummary(),
		Lab:                labKey,
	})
}

func (r *LogParserLabReconciler) selectActivity(completed []string, requested string) (challenges.Activity, []string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return challenges.Select(r.rng, completed, requested)
}

func (r *LogParserLabReconciler) nextSeed() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Int63() + 1
}

func (r *LogParserLabReconciler) setActiveChallenge(labKey, challengeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeChallenge == nil {
		r.activeChallenge = make(map[string]string)
	}
	r.activeChallenge[labKey] = challengeID
}

func (r *LogParserLabReconciler) clearActiveChallenge(labKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeChallenge != nil {
		delete(r.activeChallenge, labKey)
	}
}

func (r *LogParserLabReconciler) challengeID(labKey string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeChallenge == nil {
		r.activeChallenge = make(map[string]string)
	}
	if id := r.activeChallenge[labKey]; id != "" {
		return id
	}
	id := fmt.Sprintf("%s-pending", strings.ReplaceAll(labKey, "/", "-"))
	r.activeChallenge[labKey] = id
	return id
}

func (r *LogParserLabReconciler) setLastSubmission(labKey, answer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSubmission == nil {
		r.lastSubmission = make(map[string]string)
	}
	r.lastSubmission[labKey] = answer
}

func (r *LogParserLabReconciler) lastSubmitted(labKey string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSubmission == nil {
		return ""
	}
	return r.lastSubmission[labKey]
}

func (r *LogParserLabReconciler) clearLastSubmission(labKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSubmission != nil {
		delete(r.lastSubmission, labKey)
	}
}

func (r *LogParserLabReconciler) clearTracking(labKey string) {
	r.clearLastSubmission(labKey)
	r.clearActiveChallenge(labKey)
}

// SetupWithManager sets up the controller with the Manager.
func (r *LogParserLabReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	return ctrl.NewControllerManagedBy(mgr).
		For(&labv1alpha1.LogParserLab{}).
		Complete(r)
}
