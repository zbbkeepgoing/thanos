package huawei

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/kit/log/level"

	"github.com/go-kit/kit/log"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"gopkg.in/yaml.v2"
)

// Part size for multi part upload.
const PartSize = 1024 * 1024 * 100 //1024 * 1024 * 100

type Config struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type Bucket struct {
	name   string
	logger log.Logger
	client *obs.ObsClient
	config Config
}

func (c *Config) validate() error {
	if c.Bucket == "" {
		return errors.New("no Huawei OBS Bucket specified")
	}
	if c.Endpoint == "" {
		return errors.New("no Huawei Endpoint Specified")
	}
	if c.AccessKey != "" && c.SecretKey == "" ||
		c.AccessKey == "" && c.SecretKey != "" {
		return errors.New("must supply both an Access Key and Secret Key or neither")
	}
	return nil
}

func NewBucket(logger log.Logger, conf []byte, _ string) (*Bucket, error) {
	var config Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, errors.Wrap(err, "parse huawei obs config file failed")
	}
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "obs configuration validate failed")
	}
	obsClient, err := obs.New(config.AccessKey, config.SecretKey, config.Endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "new huawei obs client failed")
	}
	return &Bucket{
		name:   config.Bucket,
		logger: logger,
		client: obsClient,
		config: config,
	}, nil
}

func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	size, err := objstore.TryToGetSize(r)
	if err != nil {
		return errors.Wrapf(err, "failed to get size to upload %s", name)
	}
	partsNum, lastSize := int(math.Floor(float64(size)/PartSize)), size%PartSize

	ncloser := ioutil.NopCloser(r)
	switch partsNum {
	case 0:
		if _, err := b.client.PutObject(&obs.PutObjectInput{
			Body: ncloser,
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket: b.name,
					Key:    name,
				},
			},
		}, nil); err != nil {
			return errors.Wrap(err, "failed upload object to obs")
		}
	default:
		init, err := b.client.InitiateMultipartUpload(&obs.InitiateMultipartUploadInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: b.name,
				Key:    name,
			},
		}, nil)
		if err != nil {
			return errors.Wrap(err, "failed to initiate multi-part upload in huawei obs")
		}

		if lastSize != 0 {
			partsNum++
		}
		parts := make([]obs.Part, 0, partsNum)
		part := 0

		uploadPart := func(partsize int64, partIndex int) error {
			currPartSzie := partsize
			offset := int64(partIndex) * partsize
			// last part
			if partIndex+1 == partsNum {
				currPartSzie = size - offset
			}
			level.Debug(b.logger).Log("msg", "upload data", "Part Number:", partIndex+1, "Part offset:", offset, "Part Size:", currPartSzie)
			outputPart, err := b.client.UploadPart(&obs.UploadPartInput{
				Bucket:     b.name,
				Key:        init.Key,
				PartNumber: partIndex + 1,
				UploadId:   init.UploadId,
				PartSize:   currPartSzie,
				Body:       ncloser,
				Offset:     offset,
			})
			if err != nil {
				_, err := b.client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
					Bucket:   b.name,
					UploadId: init.UploadId,
					Key:      init.Key,
				})
				if err != nil {
					return errors.Wrap(err, "failed to abort mutil-part upload")
				}
				return errors.Wrap(err, "failed to upload mutil-part upload")
			}
			level.Debug(b.logger).Log("msg", "upload status", "Part Number:", partIndex+1, "upload success")
			parts = append(parts, obs.Part{
				PartNumber: outputPart.PartNumber,
				ETag:       outputPart.ETag,
			})
			return nil
		}
		for ; part < partsNum; part++ {
			err := uploadPart(PartSize, part)
			if err != nil {
				return err
			}
		}
		if _, err = b.client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
			Bucket:   b.name,
			Key:      init.Key,
			UploadId: init.UploadId,
			Parts:    parts,
		}); err != nil {
			return errors.Wrap(err, "failed to set multi-part upload completive")
		}
	}
	return nil
}

func (b *Bucket) Delete(ctx context.Context, name string) error {
	if _, err := b.client.DeleteObject(&obs.DeleteObjectInput{
		Bucket: b.name,
		Key:    name,
	}, nil); err != nil {
		return err
	}
	return nil
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}

	delimiter := objstore.DirDelim
	if objstore.ApplyIterOptions(options...).Recursive {
		delimiter = ""
	}

	listObjectsInput := &obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{
			Prefix:    dir,
			Delimiter: delimiter,
		},
		Bucket: b.name,
	}
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context closed while iterating bucket")
		}
		output, err := b.client.ListObjects(listObjectsInput, nil)
		if err != io.EOF && err != nil {
			return errors.Wrap(err, "listing object failed in obs bucket")
		}
		for _, content := range output.Contents {
			if content.Key == dir || content.Key == dir+objstore.DirDelim {
				level.Debug(b.logger).Log("skip owner object:", content.Key)
				continue
			}
			// when obs is parallel file system, directory will return in output.content. need skip it
			if content.Key[len(content.Key)-1:] == objstore.DirDelim && content.Size == 0 {
				level.Debug(b.logger).Log("skip directory:", content.Key)
				continue
			}
			level.Debug(b.logger).Log("object name:", content.Key)
			if err := f(content.Key); err != nil {
				return errors.Wrapf(err, "callback func invoke for object %s failed ", content.Key)
			}
		}
		for _, commonPrefix := range output.CommonPrefixes {
			if commonPrefix == dir || commonPrefix == dir+"/" {
				level.Debug(b.logger).Log("skip owner directory:", commonPrefix)
				continue
			}
			level.Debug(b.logger).Log("directory:", commonPrefix)
			if err := f(commonPrefix); err != nil {
				return errors.Wrapf(err, "callback func invoke for directory %s failed", commonPrefix)
			}
		}
		if !output.IsTruncated {
			break
		}
		if output.NextMarker == "" {
			break
		}
		listObjectsInput.Marker = output.NextMarker
	}
	return nil
}

func (b *Bucket) getRange(_ context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.New("given object name should not empty")
	}
	getObjectInput := &obs.GetObjectInput{
		RangeStart: off,
		RangeEnd:   off + length - 1,
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: b.name,
			Key:    name,
		},
	}
	if length != -1 {
		getObjectInput.RangeStart = off
		getObjectInput.RangeEnd = off + length - 1
	}
	resp, err := b.client.GetObject(getObjectInput, nil)
	if resp != nil && resp.Body != nil && length == -1 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		s := buf.String()
		cutStr := s[off:len(s)]
		return ioutil.NopCloser(bytes.NewBuffer([]byte(cutStr))), nil
	}
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

func (b *Bucket) Exists(_ context.Context, name string) (bool, error) {
	_, err := b.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: b.name,
		Key:    name,
	})
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "cloud not check if object exists")
	}
	return true, nil
}

func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch obsErr := errors.Cause(err).(type) {
	case obs.ObsError:
		if obsErr.StatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}

func (b *Bucket) Attributes(_ context.Context, name string) (objstore.ObjectAttributes, error) {
	var output *obs.GetObjectMetadataOutput
	output, err := b.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: b.name,
		Key:    name,
	})
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrap(err, "can not get object attributes")
	}
	return objstore.ObjectAttributes{
		Size:         output.ContentLength,
		LastModified: output.LastModified,
	}, nil
}

func (b *Bucket) Close() error {
	return nil
}

func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := Config{
		Endpoint:  os.Getenv("HUAWEI_ENDPOINT"),
		Bucket:    os.Getenv("HUAWEI_BUCKET"),
		AccessKey: os.Getenv("HUAWEI_ACCESS_KEY"),
		SecretKey: os.Getenv("HUAWEI_SECRET_KEY"),
	}
	if err := c.validateForTest(); err != nil {
		return nil, nil, errors.Wrap(err, "insufficient huawei obs configuration information")
	}
	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") != "true" {
		return nil, nil, errors.New("HUAWEI_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset HUAWEI_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
			"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
			"to safety (accidentally pointing prod bucket for test) as well as huawei obs not being fully strong consistent.")
	}
	return NewTestBucketFromConfig(t, c, true)
}

func NewTestBucketFromConfig(t testing.TB, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	ctx := context.Background()

	if c.Bucket != "" && reuseBucket {
		bc, err := yaml.Marshal(c)
		if err != nil {
			return nil, nil, err
		}
		b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
		if err != nil {
			return nil, nil, err
		}
		if err := b.Iter(ctx, "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "obs check bucket %s", c.Bucket)
		}
		t.Log("WARNING. Reusing", c.Bucket, "Huawei obs for huawei tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}
	if c.Bucket == "" {
		c.Bucket = objstore.CreateTemporaryTestBucketName(t)
	}
	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}
	b.name = c.Bucket

	if _, err := b.client.CreateBucket(&obs.CreateBucketInput{
		Bucket: c.Bucket,
		BucketLocation: obs.BucketLocation{
			Location: "ap-southeast-2",
		},
	}); err != nil {
		return nil, nil, errors.Wrapf(err, "create huawei obs bucket %s failed", c.Bucket)
	}
	return b, func() {
		objstore.EmptyBucket(t, ctx, b)
		if _, err := b.client.DeleteBucket(c.Bucket); err != nil {
			t.Logf("deleting bucket %s failed: %s", c.Bucket, err)
		}
	}, nil

}

func (c *Config) validateForTest() error {
	if c.Endpoint == "" {
		return errors.New("no Huawei Endpoint Specified")
	}
	if c.AccessKey != "" && c.SecretKey == "" ||
		c.AccessKey == "" && c.SecretKey != "" {
		return errors.New("must supply both an Access Key and Secret Key or neither")
	}
	return nil
}
