package main

import (
	"fmt"
	"strings"
)

func main() {
	fmt.Println(strings.SplitN("s:3:3", ":", 2))
	fmt.Println(strings.SplitN("s3", ":", 2))

}
