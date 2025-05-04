package utils

import (
	"errors"
	"fmt"
	"strings"
)

// Errs is an error that collects other errors, for when you want to do
// several things and then report all of them.
type Errs struct {
	errors []error
}

func (e *Errs) Add(err error) {
	if err != nil {
		e.errors = append(e.errors, err)
	}
}

func (e *Errs) Ret() error {
	if e == nil || e.IsEmpty() {
		return nil
	}
	return e
}

func (e *Errs) IsEmpty() bool {
	return e.Len() == 0
}

func (e *Errs) Len() int {
	return len(e.errors)
}

func (e *Errs) Error() string {
	asStr := make([]string, len(e.errors))
	for i, x := range e.errors {
		asStr[i] = x.Error() + "\n" + fmt.Sprintf("%+v", x) // 打印错误和栈信息
	}
	return strings.Join(asStr, ". ")
}

func (e *Errs) Is(target error) bool {
	for _, candidate := range e.errors {
		if errors.Is(candidate, target) {
			return true
		}
	}
	return false
}

func (e *Errs) As(target interface{}) bool {
	for _, candidate := range e.errors {
		if errors.As(candidate, &target) {
			return true
		}
	}
	return false
}

type ErrCode int

type CustomError struct {
	Code    ErrCode
	Message string
}

// Error implements the error interface for CustomError.
func (e *CustomError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.Code, e.Message)
}

const (
	NonIndexExisted ErrCode = 1000
)

// NewCustomError creates a new CustomError with the given code and message.
func NewCustomError(code ErrCode, format string, args ...any) error {
	return &CustomError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

func IsCustomError(err error, code ErrCode) bool {
	var customErr *CustomError
	if errors.As(err, &customErr) {
		return customErr.Code == code
	}
	return false
}
