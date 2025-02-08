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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/kr/pretty"
	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Return server stats",
	Long:  "Return server stats",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		addr, err := cmd.Flags().GetString("addr")
		if err != nil {
			return
		}

		c, err := newClient(addr)
		if err != nil {
			return
		}

		stats, err := c.stats()
		if err != nil {
			return
		}

		fmt.Printf("xyt server (version %s, running as pid %d by %s)\n",
			stats.Version.Ref, stats.Host.Pid, stats.Host.User,
		)

		fmt.Println()

		fmt.Printf("version: %s\n", stats.Version.Ref)
		fmt.Printf("built by: %s\n", stats.Version.BuildUser)
		fmt.Printf("build on: %s\n", stats.Version.BuiltOn)

		fmt.Println()

		fmt.Printf("hostname: %s\n", stats.Host.Hostname)
		fmt.Printf("uptime: %s\n", time.Duration(time.Second*time.Duration(stats.Host.Uptime)))
		fmt.Printf("memory usage: %s/%s\n",
			humanize.Bytes(stats.Host.Memstats.AllocatedBytes), humanize.Bytes(stats.Host.Memstats.SystemBytes),
		)

		fmt.Println()

		for name, schema := range stats.Datasets {
			fmt.Println(name)
			pretty.Print(schema)
			fmt.Println()
		}

		return
	},
}

func init() {
	clientCmd.AddCommand(statsCmd)
}
