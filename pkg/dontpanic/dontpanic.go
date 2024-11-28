package dontpanic

import "github.com/pkg/errors"

func DontPanic(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				err = errors.Wrap(err, "recovered panic")
			} else {
				err = errors.Errorf("recovered panic: %v", r)
			}
		}
	}()
	err = f()
	return
}
