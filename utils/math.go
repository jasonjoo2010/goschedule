package utils

func Max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func Max64(i, j int64) int64 {
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

func Min64(i, j int64) int64 {
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

func Abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
