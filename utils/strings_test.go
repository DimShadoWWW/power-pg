package utils

import "testing"

func TestSplitString(t *testing.T) {
	type a map[int]string
	type si []int
	cases := []struct {
		in   string
		size si
		want a
	}{
		{in: "Hello, 1 world 1 2 3", want: a{0: "Hello", 1: ",", 2: "1", 3: "world", 4: "1", 5: "2", 6: "3"}, size: si{0, 1, 2, 3, 4, 5, 6}},
		{in: "Hello, 1 world 1 3", want: a{0: "Hello", 1: ",", 2: "1", 3: "world", 4: "1", 5: "3"}, size: si{0, 1, 2, 3, 4, 5}},
		{in: "Hello, world", want: a{0: "Hello", 1: ",", 2: "world"}, size: si{0, 1, 2}},
	}
	for _, c := range cases {
		got, s := SplitString(c.in, " ")
		if len(s) != len(c.size) {
			t.Errorf("SplitString(%q, \" \") == \n%q\n, want \n%q", c.in, got, c.want)
		} else {
			for _, i := range s {
				if got[i] != c.want[i] {
					t.Errorf("SplitString(%q, \" \") == \n%q\n, want \n%q", c.in, got, c.want)
				}
			}
		}
	}
}

func TestGetVariables(t *testing.T) {
	cases := []struct {
		in1, in2, want string
	}{
		{"Hello, 1 world 1 2 3", "Hello, 2 world 1 2 3", "Hello, XXXXXX world 1 2 3"},
		{"Hello, 1 world 1 22 3", "Hello, 1 world 1 2 3", "Hello, 1 world 1 XXXXXX 3"},
		{"Hello, 0 world", "Hello, 2 world", "Hello, XXXXXX world"},
	}
	for _, c := range cases {
		got := GetVariables(c.in1, c.in2, 2)
		if got != c.want {
			t.Errorf("GetVariables(\n%q, \n%q, 2) == \n%q\n, want \n%q", c.in1, c.in2, got, c.want)
		}
	}
}
