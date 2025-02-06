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
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/xyt-db/xyt/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Connect to a xyt",
	Long:  "Connect to a xyt",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		return cmd.Usage()
	},
}

func init() {
	rootCmd.AddCommand(clientCmd)

	clientCmd.PersistentFlags().StringP("addr", "a", "localhost:8888", "Address on which to connect")
}

type client struct {
	server.XytClient
}

func newClient(addr string) (c client, err error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}

	c.XytClient = server.NewXytClient(conn)

	return
}

func (c client) addSchema(name string, xmin, xmax, ymin, ymax int32) (err error) {
	_, err = c.AddSchema(context.Background(), &server.Schema{
		Dataset: name,
		XMin:    xmin,
		XMax:    xmax,
		YMin:    ymin,
		YMax:    ymax,
	})

	return
}

func (c client) insert(dataset, name string, value int64, x, y, t int32) (err error) {
	cc, err := c.Insert(context.Background())
	if err != nil {
		return
	}

	err = cc.Send(&server.Record{
		Meta: &server.Metadata{
			When: timestamppb.Now(),
		},
		Dataset: dataset,
		Name:    name,
		Value:   value,
		X:       x,
		Y:       y,
		T:       t,
	})
	if err != nil {
		return
	}

	_, err = cc.CloseAndRecv()

	return
}

func (c client) query(dataset string) (err error) {
	cs, err := c.Select(context.Background(), &server.Query{
		Dataset: dataset,
	})
	if err != nil {
		return
	}

	defer func() {
		cerr := cs.CloseSend()

		switch err {
		case nil:
			err = cerr
		default:
			if cerr != nil {
				err = errors.Join(err, cerr)
			}
		}
	}()

	var record *server.Record
	for {
		record, err = cs.Recv()
		if err != nil {
			if err == io.EOF {
				err = nil
			}

			return
		}

		fmt.Printf("%#v\n", record)
	}
}
