package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type excludePatterns []string

func (patterns *excludePatterns) Set(value string) error {
	if value == "" {
		return errors.New("exclude pattern must not be empty")
	}
	*patterns = append(*patterns, value)
	return nil
}

func (patterns *excludePatterns) String() string {
	if patterns == nil {
		return ""
	}
	return strings.Join(*patterns, ",")
}

type lockWait struct {
	duration time.Duration
	infinite bool
	explicit bool
}

func (w *lockWait) Set(value string) error {
	w.explicit = true
	if value == "inf" {
		w.duration = 0
		w.infinite = true
		return nil
	}
	if value == "0" {
		w.duration = 0
		w.infinite = false
		return nil
	}
	if value == "" {
		return errors.New("lock wait must be 0, a duration, or inf")
	}
	if strings.Contains(value, "ms") {
		return fmt.Errorf("invalid lock wait %q: use only s, m, and h duration units", value)
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character != '.' && character != 's' && character != 'm' && character != 'h' {
				return fmt.Errorf("invalid lock wait %q: use only s, m, and h duration units", value)
			}
		}
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fmt.Errorf("invalid lock wait %q: use 0, a positive duration with s/m/h units, or inf", value)
	}
	w.duration = duration
	w.infinite = false
	return nil
}

func (w *lockWait) String() string {
	if w == nil {
		return "default"
	}
	if w.infinite {
		return "inf"
	}
	if w.duration == 0 {
		if !w.explicit {
			return "default"
		}
		return "0"
	}
	return strings.TrimSpace(w.duration.String())
}
