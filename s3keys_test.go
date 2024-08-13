package main

import "testing"

func TestS3SplitKey(t *testing.T) {
	tests := [][3]string{
		{"a", "e", "c"},
		{"xxxa", "xxxe", "xxxc"},
		{"", "foo", "C"},
		{"bi", "c", "bt"},
	}
	for _, test := range tests {
		if k, err := S3SplitKey(test[0], test[1]); err != nil || k != test[2] {
			t.Errorf(`"%v", "%v" => "%v", expected: "%v"`, test[0], test[1], k, test[2])
		}
	}
}
