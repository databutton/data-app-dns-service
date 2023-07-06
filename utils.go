package dataappdnsservice

import "fmt"

func DontPanic(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	err = f()
	return
}
