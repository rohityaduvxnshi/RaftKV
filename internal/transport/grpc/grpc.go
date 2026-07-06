// Package grpc is the gRPC-backed raft.Transport for real multi-process
// deployment. The Raft core is unchanged — this only maps the raft.Transport and
// raft.RPCHandler interfaces onto a gRPC service whose protobuf messages mirror
// the Figure-2 RPCs. The same core therefore passes the deterministic in-mem
// adversarial tests and runs a real networked cluster, just by swapping this in.
//
// Connections are insecure (no TLS): inter-node traffic is expected to run on a
// trusted network / overlay. TLS is a straightforward future add on the dial
// options and server creds.
package grpc

import (
	"context"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	pb "github.com/rohityaduvxnshi/RaftKV/internal/transport/grpc/proto"
)

// --- Server: a node's inbound endpoint, dispatching gRPC calls to its handler ---

// Server hosts one Raft node's RPC service.
type Server struct {
	pb.UnimplementedRaftServer
	h  raft.RPCHandler
	gs *grpc.Server
}

// NewServer wraps a node's RPCHandler (its *raft.Raft) in a gRPC service.
func NewServer(h raft.RPCHandler) *Server {
	s := &Server{h: h, gs: grpc.NewServer()}
	pb.RegisterRaftServer(s.gs, s)
	return s
}

// Serve accepts connections on lis until Stop. Run it in its own goroutine.
func (s *Server) Serve(lis net.Listener) error { return s.gs.Serve(lis) }

// Stop immediately stops serving.
func (s *Server) Stop() { s.gs.Stop() }

func (s *Server) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	reply := s.h.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  int(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &pb.RequestVoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *Server) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	reply := s.h.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      fromPBEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	return &pb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

func (s *Server) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	reply := s.h.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          int(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	return &pb.InstallSnapshotResponse{Term: reply.Term}, nil
}

// --- Transport: dials peers, implements raft.Transport ---

// Transport is one node's outbound view of the cluster: it lazily dials peers by
// ID (via the addrs map) and caches the connections.
type Transport struct {
	self  int
	addrs map[int]string
	mu    sync.Mutex
	conns map[int]*grpc.ClientConn
}

// NewTransport builds a transport for node self. addrs maps every peer ID to its
// host:port; it is read-only after construction.
func NewTransport(self int, addrs map[int]string) *Transport {
	return &Transport{self: self, addrs: addrs, conns: make(map[int]*grpc.ClientConn)}
}

var _ raft.Transport = (*Transport)(nil)

func (t *Transport) client(peer int) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return pb.NewRaftClient(c), nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, raft.ErrUnreachable
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[peer] = conn
	return pb.NewRaftClient(conn), nil
}

// Close tears down all cached connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	t.conns = make(map[int]*grpc.ClientConn)
}

func (t *Transport) SendRequestVote(ctx context.Context, peer int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	resp, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  int32(args.CandidateID),
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *Transport) SendAppendEntries(ctx context.Context, peer int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	resp, err := c.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     int32(args.LeaderID),
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      toPBEntries(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func (t *Transport) SendInstallSnapshot(ctx context.Context, peer int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	resp, err := c.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		Term:              args.Term,
		LeaderId:          int32(args.LeaderID),
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		return nil, raft.ErrUnreachable
	}
	return &raft.InstallSnapshotReply{Term: resp.Term}, nil
}

// --- conversions ---

func toPBEntries(es []raft.LogEntry) []*pb.LogEntry {
	out := make([]*pb.LogEntry, len(es))
	for i, e := range es {
		out[i] = &pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command, NoOp: e.NoOp}
	}
	return out
}

func fromPBEntries(es []*pb.LogEntry) []raft.LogEntry {
	out := make([]raft.LogEntry, len(es))
	for i, e := range es {
		out[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command, NoOp: e.NoOp}
	}
	return out
}
