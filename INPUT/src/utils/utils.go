package utils

import "strconv"

func ConvertStringInt(str string) int {
	conversor, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return conversor
}

func ConvertStringFlt(str string) float64 {
	conversor, err := strconv.ParseFloat(str, 64)
	if err != nil {
		panic(err)
	}
	return conversor
}

// verificar a length se é igual a um
