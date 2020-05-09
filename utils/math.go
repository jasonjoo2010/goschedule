package utils

func Max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func Min(i, j int) int {
	if i < j {
		return i
	}
	return j
}

func Abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
