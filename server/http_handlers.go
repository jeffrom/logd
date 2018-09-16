package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"strings"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/transport"
)

type logHandler struct {
	conf *config.Config
	h    transport.RequestHandler
}

func (h *logHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	logdreq, err := h.readRequest(req)
	if err != nil {
		panic(err)
	}

	resp, err := h.h.PushRequest(ctx, logdreq)
	if err != nil {
		panic(err)
	}

	if _, err := h.respond(w, req, resp); err != nil {
		panic(err)
	}
}

func (h *logHandler) readRequest(req *http.Request) (*protocol.Request, error) {
	ct, err := negotiateContentType(req.Header.Get("Content-type"))
	if err != nil {
		return nil, err
	}

	logdreq := protocol.NewRequest().WithConfig(h.conf)
	switch ct {
	case "application/logd":
		if _, err := logdreq.ReadFrom(bufio.NewReader(req.Body)); err != nil {
			return nil, err
		}
		return logdreq, nil
	default:
		return nil, errors.New("not supported")
	}
}

func (h *logHandler) respond(rw http.ResponseWriter, req *http.Request, resp *protocol.Response) (int64, error) {
	ct, err := negotiateContentType(req.Header.Get("Accept"))
	if err != nil {
		return 0, err
	}

	switch ct {
	case "application/logd":
		return h.respondLogd(rw, req, resp)
	default:
		return 0, errors.New("not supported")
	}
}

func (h *logHandler) respondLogd(rw http.ResponseWriter, req *http.Request, resp *protocol.Response) (int64, error) {
	var readOne bool
	var total int64
	var r io.ReadCloser
	var err error

	for {
		r, err = resp.ScanReader()
		if err != nil || r == nil {
			break
		}
		readOne = true

		n, rerr := io.Copy(rw, r)
		total += n
		if rerr != nil {
			return total, rerr
		}
	}

	if !readOne {
		log.Printf("%s: no readers in Response", req.RemoteAddr)
		// TODO should be a protocol.Err error
		return total, errors.New("internal server error")
	}
	return total, nil
}

var defaultContentType = "application/logd"

var availableContentTypes = []string{
	"application/logd",
	// "application/json",
}

func negotiateContentType(header string) (string, error) {
	if header == "" || header == "*/*" {
		return defaultContentType, nil
	}

	cts := make(map[string]bool)
	parts := strings.Split(header, ",")
	for _, part := range parts {
		mt, _, err := mime.ParseMediaType(part)
		if err == nil {
			cts[mt] = true
		}
	}

	for _, avail := range availableContentTypes {
		if ok, _ := cts[avail]; ok {
			return avail, nil
		}
	}

	return "", errors.New("not supported")
}
