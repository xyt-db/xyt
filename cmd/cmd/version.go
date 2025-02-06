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
	"fmt"

	"github.com/spf13/cobra"
)

var (
	Ref, BuildUser, BuiltOn string
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:          "version",
	Short:        "Print both server and local verisons",
	Long:         "Print both server and local verisons",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		fmt.Print("Client:\n------\n")
		fmt.Printf("version: %s\n", Ref)
		fmt.Printf("built by: %s\n", BuildUser)
		fmt.Printf("build on: %s\n", BuiltOn)

		fmt.Printf("\nServer:\n------\n")

		addr, err := cmd.Flags().GetString("addr")
		if err != nil {
			return
		}

		c, err := newClient(addr)
		if err != nil {
			return
		}

		ref, user, when, err := c.version()
		if err != nil {
			return
		}

		fmt.Printf("version: %s\n", ref)
		fmt.Printf("built by: %s\n", user)
		fmt.Printf("build on: %s\n", when)

		return
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)

	versionCmd.PersistentFlags().StringP("addr", "a", "localhost:8888", "Address on which to connect")
}
