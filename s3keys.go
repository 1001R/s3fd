package main

import (
	"errors"
	"unicode/utf8"
)

const (
	MIN_KEY rune = 32
	MAX_KEY rune = 127
)

var ErrInvalidEncoding error = errors.New("invalid encoding")

func S3SplitKey(minKey, maxKey string) (string, error) {
	if minKey >= maxKey {
		return "", nil
	}
	minKeyBytes := len(minKey)
	maxKeyBytes := len(maxKey)
	i := 0
	is := 0
	j := 0
	js := 0
	for {
		r1 := MAX_KEY
		r2 := MAX_KEY
		if i < minKeyBytes {
			if r, s := utf8.DecodeRuneInString(minKey[i:]); r != utf8.RuneError {
				r1 = r
				is = s
			} else {
				return "", ErrInvalidEncoding
			}
		}
		if j < maxKeyBytes {
			if r, s := utf8.DecodeRuneInString(maxKey[j:]); r != utf8.RuneError {
				r2 = r
				js = s
			} else {
				return "", ErrInvalidEncoding
			}
		}
		if r1 != r2 {
			if r2 == r1+1 {
				r := MIN_KEY
				if i+is < minKeyBytes {
					if r, _ = utf8.DecodeRuneInString(minKey[i+is:]); r == utf8.RuneError {
						return "", ErrInvalidEncoding
					}
				}
				return string(utf8.AppendRune([]byte(minKey[:(i+is)]), (r+MAX_KEY)/2)), nil
			}
			r := r1
			if r == MAX_KEY {
				r = MIN_KEY
			}
			return string(utf8.AppendRune([]byte(minKey[:i]), (r+r2)/2)), nil
		}
		if r1 == MAX_KEY && r2 == MAX_KEY {
			return "", nil
		}
		i += is
		j += js
	}
}
