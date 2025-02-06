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

// addSchemaCmd represents the addSchema command
var addSchemaCmd = &cobra.Command{
	Use:   "add-schema",
	Short: "Add a new schema",
	Long:  "Add a new schema",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		addr, err := cmd.Flags().GetString("addr")
		if err != nil {
			return
		}

		c, err := newClient(addr)
		if err != nil {
			return
		}

		ds, err := cmd.Flags().GetString("dataset")
		if err != nil {
			return
		}

		ints := make(map[string]int32)
		for _, f := range []string{"xmin", "xmax", "ymin", "ymax"} {
			ints[f], err = cmd.Flags().GetInt32(f)
			if err != nil {
				return
			}
		}

		return c.addSchema(ds, ints["xmin"], ints["xmax"], ints["ymin"], ints["ymax"])
	},
}

func init() {
	clientCmd.AddCommand(addSchemaCmd)

	addSchemaCmd.Flags().String("dataset", "", "The dataset name for the schema to be created")
	addSchemaCmd.Flags().Int32("xmin", 0, "The lowest value for the X column")
	addSchemaCmd.Flags().Int32("xmax", 10, "The highest value for the X column")
	addSchemaCmd.Flags().Int32("ymin", 0, "The lowest value for the Y column")
	addSchemaCmd.Flags().Int32("ymax", 10, "The highest value for the Y column")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addSchemaCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addSchemaCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
