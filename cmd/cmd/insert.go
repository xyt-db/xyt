/*
Copyright © 2025 jspc <james@zero-internet.org.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package cmd

import (
	"github.com/spf13/cobra"
)

// insertCmd represents the insert command
var insertCmd = &cobra.Command{
	Use:   "insert",
	Short: "insert some data",
	Long:  "insert some data",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		addr, err := cmd.Flags().GetString("addr")
		if err != nil {
			return
		}

		c, err := newClient(addr)
		if err != nil {
			return
		}

		strings := make(map[string]string)
		for _, f := range []string{"dataset", "name"} {
			strings[f], err = cmd.Flags().GetString(f)
			if err != nil {
				return
			}
		}

		ints := make(map[string]int32)
		for _, f := range []string{"x", "y", "t"} {
			ints[f], err = cmd.Flags().GetInt32(f)
			if err != nil {
				return
			}
		}

		value, err := cmd.Flags().GetFloat64("value")
		if err != nil {
			return
		}

		return c.insert(strings["dataset"], strings["name"], value, ints["x"], ints["y"], ints["t"])
	},
}

func init() {
	clientCmd.AddCommand(insertCmd)

	insertCmd.Flags().String("dataset", "", "The dataset name for this metric")
	insertCmd.Flags().String("name", "", "The name of this metric")
	insertCmd.Flags().Float64("value", 0, "The value of this metric")
	insertCmd.Flags().Int32P("x", "x", 0, "The X position")
	insertCmd.Flags().Int32P("y", "y", 0, "The Y position")
	insertCmd.Flags().Int32P("t", "t", 0, "The Theta position (in degs)")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// insertCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// insertCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
