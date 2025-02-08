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
	"io"
	"net"
	"os"
	"os/user"
	"runtime"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/spf13/cobra"
	"github.com/xyt-db/xyt"
	"github.com/xyt-db/xyt/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	server.UnimplementedXytServer

	database *xyt.Database
	hostname string
	user     string
}

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the xyt gRPC server",
	Long:  "Start the xyt gRPC server",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		logger, err := zap.NewProduction()
		if err != nil {
			panic(err)
		}

		sugar := logger.Sugar()

		l, err := cmd.Flags().GetString("listen")
		if err != nil {
			return
		}

		s, err := newServer()
		if err != nil {
			return
		}

		lis, err := net.Listen("tcp", l)
		if err != nil {
			return
		}

		grpcServer := grpc.NewServer(
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger),
			)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger),
			)),
		)
		server.RegisterXytServer(grpcServer, s)

		sugar.Infof("Starting a xyt server at %s", l)

		return grpcServer.Serve(lis)
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.PersistentFlags().StringP("listen", "l", "localhost:8888", "Address on which to create listener")
}

func newServer() (s *Server, err error) {
	s = new(Server)
	s.database, err = xyt.New()
	if err != nil {
		return
	}

	s.hostname, err = os.Hostname()
	if err != nil {
		return
	}

	u, err := user.Current()
	if err != nil {
		return
	}

	s.user = u.Username

	return
}

func (s *Server) AddSchema(_ context.Context, schema *server.Schema) (_ *emptypb.Empty, err error) {
	err = s.database.CreateDataset(schema)

	return
}

func (s *Server) Insert(cs grpc.ClientStreamingServer[server.Record, emptypb.Empty]) (err error) {
	var record *server.Record

	for {
		record, err = cs.Recv()
		if err != nil {
			if err == io.EOF {
				err = nil
			}

			return
		}

		err = s.database.InsertRecord(record)
		if err != nil {
			return
		}

		err = cs.SendMsg(new(emptypb.Empty))
		if err != nil {
			return
		}
	}
}

func (s *Server) Select(q *server.Query, ss grpc.ServerStreamingServer[server.Record]) (err error) {
	records, err := s.database.RetrieveRecords(q)
	if err != nil {
		return
	}

	for _, record := range records {
		err = ss.Send(record)
		if err != nil {
			if err == io.EOF {
				err = nil
			}

			return
		}
	}

	return
}

func (s *Server) Version(context.Context, *emptypb.Empty) (*server.VersionMessage, error) {
	return &server.VersionMessage{
		Ref:       Ref,
		BuildUser: BuildUser,
		BuiltOn:   BuiltOn,
	}, nil
}

func (s *Server) Stats(context.Context, *emptypb.Empty) (*server.StatsMessage, error) {
	uptime, err := host.Uptime()
	if err != nil {
		uptime = 0
	}

	ms := new(runtime.MemStats)
	runtime.ReadMemStats(ms)

	return &server.StatsMessage{
		Host: &server.Host{
			Hostname: s.hostname,
			User:     s.user,
			Uptime:   int64(uptime),
			Memstats: &server.Memstats{
				AllocatedBytes: ms.Alloc,
				SystemBytes:    ms.Sys,
			},
			Pid: int64(os.Getpid()),
		},
		Version: &server.VersionMessage{
			Ref:       Ref,
			BuildUser: BuildUser,
			BuiltOn:   BuiltOn,
		},
		Datasets: s.database.Datasets(),
	}, nil
}
