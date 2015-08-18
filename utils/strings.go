package utils

import (
	"bytes"
	"fmt"
	"sort"
)

// SplitString split string in a map, keeping order in map's index
func SplitString(s, sep string) (map[int]string, Series) {
	c := sep[0]
	symbols := []byte("(),")
	start := 0
	a := make(map[int]string)
	var idx Series
	na := 0
	idx = append(idx, na)
	for i := 0; i+len(sep) <= len(s); i++ {
		if bytes.Contains(symbols, []byte{s[i]}) {
			start++
			na++
			idx = append(idx, na)
			a[na] = string(s[i])
		} else {
			if s[i] == c && (len(sep) == 1 || s[i:i+len(sep)] == sep) {
				// a[na] = s[start:i]
				// na++
				start = i + len(sep)
				i += len(sep) - 1
				// every symbol will be handled like a word
				na++
				idx = append(idx, na)
			} else {
				a[na] = a[na] + string(s[i])
			}
		}
	}
	return a, idx
}

// GetVariables compare two strings and return a new one with "XXXXXX" in variabled positions
func GetVariables(in1, in2 string) string {
	result := make(map[int]string)
	offs := 0
	in1Splitted, i := SplitString(in1, " ")
	in2Splitted, j := SplitString(in2, " ")
	// the lenght of both has to be the same
	if len(i) == len(j) {
		sort.Sort(i)
		for _, idx := range i {
			// off is the pointer to the last 'not fount' position,
			// it has to be least than idx to check for match
			if offs <= idx {
				if in1Splitted[idx+offs] == in2Splitted[idx+offs] {
					// fmt.Println("FOUND EQUAL")
					result[idx] = in1Splitted[idx+offs]
				} else {
					// fmt.Println("NOT EQUAL")
					count := 0
					found := false
					for x := idx; x <= len(i) && in1Splitted[x] != in2Splitted[x]; x++ {
						count = x
						result[idx] = "XXXXXX"
						if x < len(i) && in1Splitted[x+1] != in2Splitted[x+1] {
							found = true
						}
					}
					if found {
						offs = count
					}
				}
			}
		}
	}
	finalStr := ""
	for k, idx := range i {
		// no space before this strings
		if k == 0 || result[idx] == "," || result[idx] == ")" {
			finalStr += result[idx]
		} else {
			finalStr += fmt.Sprintf(" %s", result[idx])
		}
	}
	return finalStr
}

// Series another one using an int[]
type Series []int

// Methods required by sort.Interface.
func (s Series) Len() int {
	return len(s)
}
func (s Series) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s Series) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Method for printing - sorts the elements before printing.
func (s Series) String() string {
	sort.Sort(s)
	str := "["
	for i, elem := range s {
		if i > 0 {
			str += " "
		}
		str += fmt.Sprint(elem)
	}
	return str + "]"
}
