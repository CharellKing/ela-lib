package utils

func InsertSlice[T any](slice []T, index int, value T) []T {
	if index < 0 || index > len(slice) {
		return slice // Handle out-of-bounds index gracefully
	}

	// Create a new slice with the desired capacity to avoid unnecessary reallocations
	newSlice := make([]T, len(slice)+1)

	// Copy elements before the insertion point
	copy(newSlice, slice[:index])

	// Insert the new value
	newSlice[index] = value

	// Copy elements after the insertion point
	copy(newSlice[index+1:], slice[index:])

	return newSlice
}
