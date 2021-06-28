package huawei

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"

	"github.com/go-kit/kit/log"
)

func Test_PutObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	hwConfig := Config{
		Endpoint:  "xxx",
		Bucket:    "thanos-obs-test",
		AccessKey: "accesskey",
		SecretKey: "secretkey",
	}
	hwConfig.Endpoint = srv.Listener.Addr().String()
	config, err := yaml.Marshal(hwConfig)
	b, err := NewBucket(logger, config, "")
	if err != nil {
		level.Debug(logger).Log("new Bucket error")
	}
	err = b.Upload(context.TODO(), "thanos", bytes.NewBuffer([]byte("thanos")))
	testutil.Ok(t, err)

}

func Test_DeleteObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name  string
		exist bool
		err   error
	}{
		{
			name:  "Object not Exist",
			exist: false,
			err: obs.ObsError{
				BaseModel: obs.BaseModel{
					StatusCode: http.StatusNotFound,
				},
			},
		},
		{
			name:  "Object Exist",
			exist: true,
			err:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.exist {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}

			}))
			defer srv.Close()
			hwConfig := Config{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			config, err := yaml.Marshal(hwConfig)
			b, err := NewBucket(logger, config, "")
			if err != nil {
				level.Debug(logger).Log("new Bucket error")
			}
			err = b.Delete(context.TODO(), "test")
			if !tt.exist {
				level.Debug(logger).Log("name", tt.name, "msg", "not exist")
				obsErr, ok := err.(obs.ObsError)
				actObsErr, _ := tt.err.(obs.ObsError)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, actObsErr.BaseModel.StatusCode, obsErr.BaseModel.StatusCode)
				return
			}
			level.Debug(logger).Log("name", tt.name, "msg", "exist")
			testutil.Ok(t, err)
		})
	}
}

func Test_IterObject(t *testing.T) {
	isNextMarker := true
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name         string
		isErr        bool
		isNextMarker bool
		f            func(name string) error
		output       *obs.ListObjectsOutput
		outputNext   *obs.ListObjectsOutput
	}{
		{
			name:         "no more data",
			isErr:        false,
			isNextMarker: false,
			f: func(name string) error {
				return nil
			},
			output: &obs.ListObjectsOutput{
				Name:   "thanos",
				Prefix: "thanos/",
				Contents: []obs.Content{
					{
						Key: "thanos/file1",
					},
					{
						Key: "thanos/file2",
					},
					{
						Key: "thanos/",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder1",
					"thanos/folder2",
					"thanos/folder3",
				},
			},
			outputNext: nil,
		},
		{
			name:         "more data",
			isErr:        false,
			isNextMarker: true,
			f: func(name string) error {
				return nil
			},
			output: &obs.ListObjectsOutput{
				Name:        "thanos",
				Prefix:      "thanos/",
				IsTruncated: true,
				NextMarker:  "nextMarker",
				Contents: []obs.Content{
					{
						Key: "thanos/file1",
					},
					{
						Key: "thanos/file2",
					},
					{
						Key: "thanos/",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder1",
					"thanos/folder2",
					"thanos/folder3",
				},
			},
			outputNext: &obs.ListObjectsOutput{
				Name:   "thanos",
				Prefix: "thanos/",
				Contents: []obs.Content{
					{
						Key: "thanos/file3",
					},
					{
						Key: "thanos/file4",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder4",
					"thanos/folder5",
					"thanos/folder6",
				},
			},
		},
		{
			name:         "err happen",
			isErr:        true,
			isNextMarker: false,
			f: func(name string) error {
				return errors.New("some unexpect exception happen!")
			},
			output: &obs.ListObjectsOutput{
				Name:   "thanos",
				Prefix: "thanos/",
				Contents: []obs.Content{
					{
						Key: "thanos/file1",
					},
					{
						Key: "thanos/file2",
					},
					{
						Key: "thanos/",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder1",
					"thanos/folder2",
					"thanos/folder3",
				},
			},
			outputNext: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				values := r.URL.Query()
				marker := values.Get("marker")
				var xmlBytes []byte
				var err error
				if marker == "nextMarker" {
					isNextMarker = true
					xmlBytes, err = xml.Marshal(tt.outputNext)
				} else {
					isNextMarker = false
					xmlBytes, err = xml.Marshal(tt.output)
				}
				if err != nil {
					fmt.Fprint(w, err)
				}
				w.WriteHeader(200)
				w.Write([]byte(xmlBytes))
			}))
			defer srv.Close()
			hwConfig := Config{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			config, err := yaml.Marshal(hwConfig)
			b, err := NewBucket(logger, config, "")
			if err != nil {
				logger.Log("new Bucket error")
			}
			err = b.Iter(context.TODO(), "thanos", tt.f)
			if tt.isErr {
				level.Debug(logger).Log("isErr", "yes")
				if (err != nil) != tt.isErr {
					t.Errorf("Iter() error = %v, isErr %v", err, tt.isErr)
				}
				return
			}
			level.Debug(logger).Log("isErr", "no", "isNextMarker", isNextMarker)
			testutil.Equals(t, tt.isNextMarker, isNextMarker)
		})
	}
}

func Test_GetRangeObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name   string
		exist  bool
		err    error
		object string
		length int
	}{
		{
			name:  "Object not Exist",
			exist: false,
			err: obs.ObsError{
				BaseModel: obs.BaseModel{
					StatusCode: http.StatusNotFound,
				},
			},
			object: "",
			length: -1,
		},
		{
			name:   "Object Exist & Get All Data",
			exist:  true,
			err:    nil,
			object: "getObjectFull",
			length: -1,
		},
		{
			name:   "Object Exist & Get Range Data",
			exist:  true,
			err:    nil,
			object: "getObject",
			length: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.exist {
					w.Header().Set("Last-Modified", "2021-06-24 07:14:08 +0000 GMT")
					w.Header().Set("Content-Length", "10000")

					var err error
					// Write less bytes than the content length.
					if tt.length == -1 {
						_, err = w.Write([]byte("getObjectFull"))
					} else {
						_, err = w.Write([]byte("getObject"))
					}
					testutil.Ok(t, err)
					w.WriteHeader(200)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}

			}))
			defer srv.Close()
			hwConfig := Config{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			config, err := yaml.Marshal(hwConfig)
			b, err := NewBucket(logger, config, "")
			if err != nil {
				logger.Log("new Bucket error")
			}
			var reader io.ReadCloser
			if tt.length == -1 {
				reader, err = b.Get(context.TODO(), "test")
			} else {
				reader, err = b.GetRange(context.TODO(), "test", 0, 10)
			}
			if !tt.exist {
				level.Debug(logger).Log("name", tt.name, "msg", "not exist")
				obsErr, ok := err.(obs.ObsError)
				actObsErr, _ := tt.err.(obs.ObsError)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, actObsErr.BaseModel.StatusCode, obsErr.BaseModel.StatusCode)
				return
			}
			level.Debug(logger).Log("name", tt.name, "msg", "exist")
			testutil.Ok(t, err)
			var data []byte
			// We expect an error when reading back.
			data, err = ioutil.ReadAll(reader)
			testutil.Equals(t, string(data[:]), tt.object)
		})
	}
}

func Test_ExistObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name   string
		exist  bool
		err    error
		object string
	}{
		{
			name:  "Object not Exist",
			exist: false,
			err: obs.ObsError{
				BaseModel: obs.BaseModel{
					StatusCode: http.StatusNotFound,
				},
			},
			object: "",
		},
		{
			name:   "Object Exist",
			exist:  true,
			err:    nil,
			object: "getObject",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.exist {
					w.Header().Set("Last-Modified", "2021-06-24 07:14:08 +0000 GMT")
					w.Header().Set("Content-Length", "10000")
					_, err := w.Write([]byte("getObject"))
					testutil.Ok(t, err)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			hwConfig := Config{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			config, err := yaml.Marshal(hwConfig)
			b, err := NewBucket(logger, config, "")
			if err != nil {
				level.Debug(logger).Log("new Bucket error")
			}
			reader, err := b.Get(context.TODO(), "test")
			if !tt.exist {
				level.Debug(logger).Log("name", tt.name, "msg", "not exist")
				obsErr, ok := err.(obs.ObsError)
				actObsErr, _ := tt.err.(obs.ObsError)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, actObsErr.BaseModel.StatusCode, obsErr.BaseModel.StatusCode)
				return
			}
			level.Debug(logger).Log("name", tt.name, "msg", "exist")
			testutil.Ok(t, err)
			var data []byte
			// We expect an error when reading back.
			data, err = ioutil.ReadAll(reader)
			testutil.Equals(t, string(data[:]), tt.object)
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	type hwConfig struct {
		Endpoint  string
		Bucket    string
		AccessKey string
		SecretKey string
	}
	tests := []struct {
		name     string
		isErr    bool
		hwConfig hwConfig
	}{
		{
			name:  "normal configuration",
			isErr: false,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing endpoint",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing bucket",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing access key",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "thanos-obs-test",
				AccessKey: "",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing secret key",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &Config{
				Endpoint:  tt.hwConfig.Endpoint,
				Bucket:    tt.hwConfig.Bucket,
				AccessKey: tt.hwConfig.AccessKey,
				SecretKey: tt.hwConfig.SecretKey,
			}
			err := conf.validate()
			if (err != nil) != tt.isErr {
				t.Errorf("Config.validate() error = %v, isErr %v", err, tt.isErr)
			}
		})
	}
}
