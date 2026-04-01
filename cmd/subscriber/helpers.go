package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func printJSON(data json.RawMessage) error {
	if pretty {
		var buf bytes.Buffer
		if err := json.Indent(&buf, data, "", "  "); err != nil {
			fmt.Println(string(data))
			return nil
		}
		fmt.Println(buf.String())
	} else {
		fmt.Println(string(data))
	}
	return nil
}
