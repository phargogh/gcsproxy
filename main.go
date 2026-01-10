package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"google.golang.org/api/option"
)

var (
	bind         = flag.String("b", "127.0.0.1:8080", "Bind address")
	verbose      = flag.Bool("v", false, "Show access log")
	credentials  = flag.String("c", "", "The path to the keyfile. If not present, client will use your default application credentials.")
	defaultIndex = flag.String("i", "", "The default index file to serve.")
)

var client *storage.Client

func handleError(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrObjectNotExist) {
		// http.StatusForbidden is HTTP 403 Forbidden, which GCS typically returns instead of 404
		http.Error(w, err.Error(), http.StatusForbidden)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func header(r *http.Request, key string) (string, bool) {
	if r.Header == nil {
		return "", false
	}
	if candidate := r.Header[key]; len(candidate) > 0 {
		return candidate[0], true
	}
	return "", false
}

func setStrHeader(w http.ResponseWriter, key string, value string) {
	if value != "" {
		w.Header().Add(key, value)
	}
}

func setIntHeader(w http.ResponseWriter, key string, value int64) {
	if value > 0 {
		w.Header().Add(key, strconv.FormatInt(value, 10))
	}
}

func setTimeHeader(w http.ResponseWriter, key string, value time.Time) {
	if !value.IsZero() {
		w.Header().Add(key, value.UTC().Format(http.TimeFormat))
	}
}

type wrapResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *wrapResponseWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.status = status
}

func wrapper(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proc := time.Now()
		writer := &wrapResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		fn(writer, r)
		addr := r.RemoteAddr
		if ip, found := header(r, "X-Forwarded-For"); found {
			addr = ip
		}
		if *verbose {
			log.Printf("[%s] %.3f %d %s %s",
				addr,
				time.Now().Sub(proc).Seconds(),
				writer.status,
				r.Method,
				r.URL,
			)
		}
	}
}

func fetchObjectAttrs(ctx context.Context, bucket, object string) (*storage.ObjectAttrs, error) {
	var err error
	var indexAppended bool
	if object == "" && *defaultIndex != "" {
		object, err = url.JoinPath(object, *defaultIndex)
		if err != nil {
			return nil, err
		}
		indexAppended = true
	}

	attrs, err := client.Bucket(bucket).Object(strings.TrimSuffix(object, "/")).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			if *defaultIndex == "" || indexAppended {
				return nil, err
			}
			object, err = url.JoinPath(object, *defaultIndex)
			if err != nil {
				return nil, err
			}
			return client.Bucket(bucket).Object(object).Attrs(ctx)
		}
		return nil, err
	}
	return attrs, nil
}

func proxy(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	attrs, err := fetchObjectAttrs(r.Context(), params["bucket"], params["object"])
	if err != nil {
		handleError(w, err)
		return
	}
	if lastStrs, ok := r.Header["If-Modified-Since"]; ok && len(lastStrs) > 0 {
		last, err := http.ParseTime(lastStrs[0])
		if *verbose && err != nil {
			log.Printf("could not parse If-Modified-Since: %v", err)
		}
		if !attrs.Updated.Truncate(time.Second).After(last) {
			w.WriteHeader(304)
			return
		}
	}

	if r.Method == http.MethodHead {
		setTimeHeader(w, "Last-Modified", attrs.Updated)
		setStrHeader(w, "Content-Type", attrs.ContentType)
		setStrHeader(w, "Content-Language", attrs.ContentLanguage)
		setStrHeader(w, "Cache-Control", attrs.CacheControl)
		setStrHeader(w, "Content-Encoding", attrs.ContentEncoding)
		setStrHeader(w, "Content-Disposition", attrs.ContentDisposition)
		setIntHeader(w, "Content-Length", attrs.Size)
		setStrHeader(w, "Accept-Ranges", "bytes")
		setStrHeader(w, "Server", "UploadServer")
		return
	}

	gzipAcceptable := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
	if strings.Contains(r.Header.Get("Range"), "bytes") {
		log.Printf("Range header detected: %s", r.Header.Get("Range"))
		// process the range options
		// Example range header value: bytes=0-50, 100-150, 9500-
		// TODO: "last N bytes": bytes=-42
		range_string := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		for _, item := range strings.Split(range_string, ",") {
			start_byte := int64(0)
			end_byte := int64(attrs.Size)

			has_start_byte := !strings.HasPrefix(item, "-")
			has_end_byte := !strings.HasSuffix(item, "-")

			byte_range := strings.Split(item, "-")
			if has_start_byte && has_end_byte {
				first_byte, first_err := strconv.ParseInt(byte_range[0], 10, 64)
				if first_err != nil {
					log.Printf("Could not convert first byte to int %s", byte_range[0])
				}
				second_byte, second_err := strconv.ParseInt(byte_range[1], 10, 64)
				if second_err != nil {
					log.Printf("Could not convert second byte to int %s", byte_range[0])
				}
				start_byte = first_byte
				end_byte = second_byte
			} else {
				known_byte, err := strconv.ParseInt(byte_range[0], 10, 64)
				if err != nil {
					log.Printf("Coult not convert byte to int %s", byte_range[0])
				}
				if has_start_byte { // implied: !has_end_byte
					start_byte = known_byte
				} else if has_end_byte {
					end_byte = known_byte
				} else {
					log.Printf("This should be impossible")
				}
			}

			end_byte = min(end_byte, int64(attrs.Size))
			log.Printf("Requesting range %v-%v", start_byte, end_byte)
			length := end_byte - start_byte + 1  // range is inclusive w/r/t length
			objr, err := client.Bucket(attrs.Bucket).Object(attrs.Name).ReadCompressed(gzipAcceptable).NewRangeReader(r.Context(), start_byte, length)
			if err != nil {
				handleError(w, err)
				return
			}
			w.WriteHeader(http.StatusPartialContent)  // indicate 206 partial content
			setTimeHeader(w, "Last-Modified", attrs.Updated)
			setStrHeader(w, "Content-Type", attrs.ContentType)
			setStrHeader(w, "Content-Language", attrs.ContentLanguage)
			setStrHeader(w, "Cache-Control", attrs.CacheControl)
			setStrHeader(w, "Content-Encoding", objr.Attrs.ContentEncoding)
			setStrHeader(w, "Content-Disposition", attrs.ContentDisposition)
			setStrHeader(w, "Accept-Ranges", "bytes")
			setStrHeader(w, "Server", "UploadServer")  // Google specific

			log.Printf("Content length: %v", length)
			setIntHeader(w, "Content-Length", length)
			io.Copy(w, objr)
		}
	} else {
		objr, err := client.Bucket(attrs.Bucket).Object(attrs.Name).ReadCompressed(gzipAcceptable).NewReader(r.Context())
		if err != nil {
			handleError(w, err)
			return
		}
		setTimeHeader(w, "Last-Modified", attrs.Updated)
		setStrHeader(w, "Content-Type", attrs.ContentType)
		setStrHeader(w, "Content-Language", attrs.ContentLanguage)
		setStrHeader(w, "Cache-Control", attrs.CacheControl)
		setStrHeader(w, "Content-Encoding", objr.Attrs.ContentEncoding)
		setStrHeader(w, "Content-Disposition", attrs.ContentDisposition)
		setIntHeader(w, "Content-Length", objr.Attrs.Size)
		setStrHeader(w, "Accept-Ranges", "bytes")
		setStrHeader(w, "Server", "UploadServer")
		io.Copy(w, objr)
	}
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	setStrHeader(w, "Content-Type", "text/plain")
	io.WriteString(w, "OK\n")
}

func main() {
	flag.Parse()

	var err error
	if *credentials != "" {
		client, err = storage.NewClient(context.Background(), option.WithCredentialsFile(*credentials))
	} else {
		client, err = storage.NewClient(context.Background())
	}
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/_health", wrapper(healthCheck)).Methods("GET", "HEAD")
	r.HandleFunc("/{bucket:[0-9a-zA-Z-_.]+}/{object:.*}", wrapper(proxy)).Methods("GET", "HEAD")

	log.Printf("[service] listening on %s", *bind)
	if err := http.ListenAndServe(*bind, r); err != nil {
		log.Fatal(err)
	}
}
