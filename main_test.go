package main

import "testing"

func TestString2Bool(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"", false},
		{"0", false},
		{"false", false},
		{"no", false},
		{"off", false},
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{" yes ", true},
		{"on", true},
		{"t", true},
		{"y", true},
		{"Yes", true},
	} {
		if got := string2Bool(tc.in); got != tc.want {
			t.Errorf("string2Bool(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
