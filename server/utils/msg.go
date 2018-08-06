package utils

import (
	"bytes"
	"encoding/base64"
	"errors"

	"fxdayu.com/dyupdater/server/models"
	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack"
)

func UnpackMsgpackSnappy(data []byte, v interface{}) error {
	if data[0] == 'S' {
		dst, err := snappy.Decode(nil, data[1:])
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(dst, v)
	} else if data[0] == 0 {
		return msgpack.Unmarshal(data[1:], v)
	}
	return errors.New("decode failed, unsupported message format")
}

func PackMsgpackSnappy(v ...interface{}) ([]byte, error) {
	tmp, err := msgpack.Marshal(v...)
	if err != nil {
		return nil, err
	}
	if len(tmp) > 1000 {
		tmp = snappy.Encode(nil, tmp)
		return bytes.Join([][]byte{[]byte("S"), tmp}, []byte("")), nil
	}
	return bytes.Join([][]byte{[]byte("\x00"), tmp}, []byte("")), nil
}

func ParseFactorValue(s string, data *models.FactorValue) error {
	decodeBytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	err = UnpackMsgpackSnappy(decodeBytes, &data.Values)
	if err != nil {
		return err
	}
	date, ok := data.Values["trade_date"]
	if !ok {
		return errors.New("No trade_date in result")
	}
	delete(data.Values, "trade_date")
	data.Datetime = make([]int, len(date))
	for i, v := range date {
		data.Datetime[i] = int(v)
	}
	data.DropNAN()
	return nil
}
