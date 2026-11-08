package companion

import (
	"context"
	"strings"
	"testing"
)

func TestSettingsDefaultOffAndOwnerScopedNonce(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if err := s.OpenSettings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	first, _ := f.worker.settings(ctx, 1)
	second, _ := f.worker.settings(ctx, 2)
	if first.Mode != "off" || second.Mode != "off" || len(f.requests) != 0 {
		t.Fatal("opening settings enabled automation or called source")
	}
	if _, err := s.HandleSettingsCallback(ctx, 2, "as:"+first.Nonce+":automatic"); err == nil {
		t.Fatal("cross-owner nonce accepted")
	}
	if _, err := s.HandleSettingsCallback(ctx, 1, "as:"+first.Nonce+":interval:60"); err != nil {
		t.Fatal(err)
	}
	changed, _ := f.worker.settings(ctx, 1)
	if changed.Interval != 60 || changed.Nonce == first.Nonce || changed.Mode != "off" {
		t.Fatal("setting unexpectedly enabled mode")
	}
	if _, err := s.HandleSettingsCallback(ctx, 1, "as:"+changed.Nonce+":automatic"); err != nil {
		t.Fatal(err)
	}
	enabled, _ := f.worker.settings(ctx, 1)
	if _, err := s.HandleSettingsCallback(ctx, 1, "as:"+changed.Nonce+":automatic"); err == nil {
		t.Fatal("repeat callback created another run")
	}
	again, _ := f.worker.settings(ctx, 1)
	if enabled.RunID != again.RunID {
		t.Fatal("repeated callback replaced run")
	}
}
func TestSettingsPauseAndResumeDoNotReenableOldPolicy(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if err := f.worker.Start(ctx, 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	before, _ := f.worker.settings(ctx, 1)
	if err := s.DisableAutolike(ctx, 1, "paused"); err != nil {
		t.Fatal(err)
	}
	s.Store.Pause(ctx, 1, false)
	if _, err := s.HandleSettingsCallback(ctx, 1, "as:"+before.Nonce+":automatic"); err == nil {
		t.Fatal("stale active menu reenabled after resume")
	}
	now, _ := f.worker.settings(ctx, 1)
	if now.Mode != "off" || len(f.requests) != 0 {
		t.Fatal("resume resurrected automation")
	}
}
func TestSettingsSeriesBoundsAndIntervalPresets(t *testing.T) {
	f := newAutoFixture(t)
	ctx := context.Background()
	s := f.worker.Service
	for _, command := range []string{"/autolikes 0", "/autolikes 51", "/autolike_cap 0", "/autolike_cap 501"} {
		if handled, err := s.HandleAutolikeCommand(ctx, 1, command); !handled || err == nil {
			t.Fatalf("bad command accepted: %s", command)
		}
	}
	j, _ := f.worker.settings(ctx, 1)
	if err := f.worker.updateSetting(ctx, 1, j.Nonce, "interval", 20); err == nil {
		t.Fatal("unsupported interval")
	}
	if handled, err := s.HandleAutolikeCommand(ctx, 1, "/settings"); !handled || err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s.AutolikeStatus(ctx, 1), "выключен") {
		t.Fatal("missing off status")
	}
}
