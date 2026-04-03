package xlive

import (
	"crypto/cipher"
	"errors"
	"net"

	"github.com/gofrs/uuid/v5"
	"github.com/shuffleman/xlive-tunnel/crypto"
	"github.com/shuffleman/xlive-tunnel/httpflv"
	"github.com/shuffleman/xlive-tunnel/rtmp"
)

type ClientOptions struct {
	UploadConn   net.Conn
	DownloadConn net.Conn

	DownloadHost string
	DownloadPath string

	DownloadProto string

	SharedSecret []byte
	UUID         string
}

func NewClient(options ClientOptions) (*ClientConn, error) {
	applyIOSDefaults()
	if options.UploadConn == nil || options.DownloadConn == nil {
		return nil, errors.New("xlive: missing connections")
	}
	var shared []byte
	if len(options.SharedSecret) > 0 {
		shared = options.SharedSecret
	} else if options.UUID != "" {
		id, err := uuid.FromString(options.UUID)
		if err != nil {
			return nil, err
		}
		shared = id.Bytes()
	} else {
		return nil, errors.New("xlive: missing shared secret")
	}
	keyiv, err := crypto.DeriveKeyIV(shared)
	if err != nil {
		return nil, err
	}
	enc, dec, err := crypto.NewCFBStreams(keyiv)
	if err != nil {
		return nil, err
	}
	sid, err := NewRandomSessionID()
	if err != nil {
		return nil, err
	}

	upload := rtmp.NewClient(options.UploadConn, rtmp.ClientOptions{
		ChunkSize: 262144,
		Enc:       enc,
		SessionID: sid,
	})
	err = upload.Start()
	if err != nil {
		return nil, err
	}

	var download net.Conn
	if options.DownloadProto == "rtmp" {
		rc, derr := rtmp.DialPlay(options.DownloadConn, rtmp.PlayClientOptions{
			Dec:        dec,
			SessionID:  sid,
			StreamName: "live_" + sid,
		})
		if derr != nil {
			_ = upload.Close()
			return nil, derr
		}
		download = rc
	} else {
		var derr error
		download, derr = httpflv.Dial(options.DownloadConn, httpflv.ClientOptions{
			Path: options.DownloadPath,
			Host: options.DownloadHost,
			SID:  sid,
			Dec:  dec,
		})
		if derr != nil {
			_ = upload.Close()
			return nil, derr
		}
	}
	return NewClientConn(upload, download), nil
}

func _keepCipherStream(_ cipher.Stream) {}
