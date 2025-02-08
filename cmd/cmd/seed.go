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
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/xyt-db/xyt/server"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// seedCmd represents the seed command
var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "Add a load of data for messing about with",
	Long:  `Simulate a 1000x1000 unit warehouse, and then a robot going up and down some locations`,
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		addr, err := cmd.Flags().GetString("addr")
		if err != nil {
			return
		}

		c, err := newClient(addr)
		if err != nil {
			return
		}

		ds := "superduperdataset"
		_, err = c.AddSchema(context.Background(), &server.Schema{
			Dataset:             ds,
			XMax:                1000,
			YMax:                1000,
			Frequency:           server.Frequency_F1000Hz,
			SortOnInsert:        false,
			LazyInitialAllocate: true,
		})
		if err != nil {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		cc, err := c.Insert(ctx)
		if err != nil {
			return
		}

		defer func() {
			_, cerr := cc.CloseAndRecv()
			err = errors.Join(err, cerr)
		}()

		ts := time.Now().Add(0 - time.Hour*12)

		bar := progressbar.Default(227_284)

		for x := int32(0); x < 1000; x += 15 {
			for y := int32(10); y < 1000-75; y++ {
				for _, metric := range []string{
					"temperature", "voltage", "network", "flurbles",
				} {
					err = cc.Send(&server.Record{
						Meta: &server.Metadata{
							When: timestamppb.New(ts),
							Labels: map[string]string{
								"robot": "robo-001",
							},
						},
						X:       x,
						Y:       y,
						T:       180,
						Dataset: ds,
						Name:    metric,
						// #nosec: G404
						Value: rand.Float64() * 30,
					})
					if err != nil {
						return
					}

					ts = ts.Add(time.Millisecond * time.Millisecond * 350)

					err = bar.Add(1)
					if err != nil {
						return
					}
				}
			}

			ts = ts.Add(time.Minute * 5)
		}

		return
	},
}

func init() {
	clientCmd.AddCommand(seedCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// seedCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// seedCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
