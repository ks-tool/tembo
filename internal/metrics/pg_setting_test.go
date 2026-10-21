package metrics

import "testing"

func TestNormaliseUnit(t *testing.T) {
	for _, tc := range []struct {
		setting, unit string
		wantVal       float64
		wantUnit      string
	}{
		{"8", "8kB", 8 << 13, "bytes"},
		{"16", "MB", 16 << 20, "bytes"},
		{"2", "GB", 2 << 30, "bytes"},
		{"1500", "ms", 1.5, "seconds"},
		{"5", "min", 300, "seconds"},
		{"2", "h", 7200, "seconds"},
		{"1", "d", 86400, "seconds"},
		{"-1", "min", -1, "seconds"}, // special-cased, not scaled
		{"42", "", 42, ""},
	} {
		s := &pgSetting{name: "x", setting: tc.setting, unit: tc.unit, vartype: "integer"}
		val, unit, err := s.normaliseUnit()
		if err != nil {
			t.Errorf("%s %s: %v", tc.setting, tc.unit, err)
			continue
		}
		if val != tc.wantVal || unit != tc.wantUnit {
			t.Errorf("%s %s = %g %q, want %g %q", tc.setting, tc.unit, val, unit, tc.wantVal, tc.wantUnit)
		}
	}

	s := &pgSetting{name: "x", setting: "1", unit: "quads", vartype: "integer"}
	if _, _, err := s.normaliseUnit(); err == nil {
		t.Error("an unknown unit: want an error")
	}
}
