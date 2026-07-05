package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

const requestTimeout = 3 * time.Second

// NewHTTPHandler exposes the KV service over HTTP:
//
//	GET    /kv/{key}                          linearizable read
//	PUT    /kv/{key}          body=value      write
//	DELETE /kv/{key}                          delete
//	POST   /kv/{key}/cas?expected=..&value=.. compare-and-swap
//	POST   /kv/{key}/append   body=suffix     append
//
// Client sessions travel in the X-Client-Id and X-Seq-No headers (for
// exactly-once retries). A request to a non-leader is answered with a redirect
// to the leader when its URL is known (peers maps node ID -> base URL), else 503
// with the leader ID in the X-Leader-Id header.
func NewHTTPHandler(s *Server, peers map[int]string) http.Handler {
	h := &httpHandler{s: s, peers: peers}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", h.get)
	mux.HandleFunc("PUT /kv/{key}", h.put)
	mux.HandleFunc("DELETE /kv/{key}", h.del)
	mux.HandleFunc("POST /kv/{key}/cas", h.cas)
	mux.HandleFunc("POST /kv/{key}/append", h.append)
	return mux
}

type httpHandler struct {
	s     *Server
	peers map[int]string
}

func (h *httpHandler) session(r *http.Request) (string, uint64) {
	seq, _ := strconv.ParseUint(r.Header.Get("X-Seq-No"), 10, 64)
	return r.Header.Get("X-Client-Id"), seq
}

func (h *httpHandler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		leader := h.s.Leader()
		w.Header().Set("X-Leader-Id", strconv.Itoa(leader))
		if url, ok := h.peers[leader]; ok && leader >= 0 {
			http.Redirect(w, r, url+r.URL.RequestURI(), http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, "not leader; leader unknown", http.StatusServiceUnavailable)
	case errors.Is(err, ErrTimeout):
		http.Error(w, "timed out", http.StatusGatewayTimeout)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func ctxFor(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), requestTimeout)
}

func (h *httpHandler) get(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := ctxFor(r)
	defer cancel()
	v, found, err := h.s.Get(ctx, r.PathValue("key"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"value": v})
}

func (h *httpHandler) put(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	cid, seq := h.session(r)
	ctx, cancel := ctxFor(r)
	defer cancel()
	if err := h.s.Put(ctx, cid, seq, r.PathValue("key"), string(body)); err != nil {
		h.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *httpHandler) del(w http.ResponseWriter, r *http.Request) {
	cid, seq := h.session(r)
	ctx, cancel := ctxFor(r)
	defer cancel()
	found, err := h.s.Delete(ctx, cid, seq, r.PathValue("key"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *httpHandler) cas(w http.ResponseWriter, r *http.Request) {
	cid, seq := h.session(r)
	ctx, cancel := ctxFor(r)
	defer cancel()
	swapped, err := h.s.CAS(ctx, cid, seq, r.PathValue("key"), r.URL.Query().Get("expected"), r.URL.Query().Get("value"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]bool{"swapped": swapped})
}

func (h *httpHandler) append(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	cid, seq := h.session(r)
	ctx, cancel := ctxFor(r)
	defer cancel()
	v, err := h.s.Append(ctx, cid, seq, r.PathValue("key"), string(body))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"value": v})
}
