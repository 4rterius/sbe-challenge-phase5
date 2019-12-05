package qntupdater

import (
	"errors"
	"fmt"
	"io/ioutil"
)

type ScriptConfig struct {
	DbName      string
	DbUser      string
	DbPswd      string
	TablePrefix string
}

func (c *ScriptConfig) ReadFrom(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	n, err := fmt.Sscanf(string(data[:]), "%s %s %s %s", &c.DbName, &c.DbUser, &c.DbPswd, &c.TablePrefix)

	if err != nil {
		return err
	}

	if n != 4 {
		return errors.New("Incorrect number of arguments in the config (should be 4)")
	}

	return nil
}
