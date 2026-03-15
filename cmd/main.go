/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	labv1alpha1 "github.com/lpmi-13/logparser-lab-operator/api/v1alpha1"
	"github.com/lpmi-13/logparser-lab-operator/internal/controller"
	"github.com/lpmi-13/logparser-lab-operator/internal/notifier"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(labv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var enableNotifications bool
	var notificationPort int
	var logsDir string
	var answerRoot string

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. Use 0 to disable metrics.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.BoolVar(&enableNotifications, "enable-notifications", true, "Enable the browser notification feed.")
	flag.IntVar(&notificationPort, "notification-port", 8888, "Port for the notification SSE server.")
	flag.StringVar(&logsDir, "logs-dir", "./logs", "Host path where the operator writes the single active round log.")
	flag.StringVar(&answerRoot, "answer-root", "/tmp/logparser-labs", "Host path used for answer files.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	absLogsDir, err := filepath.Abs(logsDir)
	if err != nil {
		setupLog.Error(err, "unable to resolve logs directory", "logsDir", logsDir)
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "f52d3264.learning.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	var n *notifier.Notifier
	if enableNotifications {
		n = notifier.New()
		server := notifier.NewServer(n, notificationPort)
		if err := mgr.Add(server); err != nil {
			setupLog.Error(err, "unable to add notification server")
			os.Exit(1)
		}
		setupLog.Info("Notification server enabled", "url", fmt.Sprintf("http://localhost:%d", notificationPort))
	}

	if err := (&controller.LogParserLabReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Notifier:   n,
		LogsDir:    absLogsDir,
		AnswerRoot: answerRoot,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "LogParserLab")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
