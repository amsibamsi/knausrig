package cfg

import (
	"encoding/json"
	"io/ioutil"
)

// Config holds the configuration for a MapReduce program.
//
// Addresses are used for TCP and must have the form host:port
type Config struct {

	// Local address of the master to listen on. Mappers and reducers must be
	// able to connect to this.
	Master string

	// List of local addresses, one for each mapper. The master will connect to
	// this address on port 22 via SSH and start a local mapper process to listen
	// on this address. The master must be able to connect to this address.
	Mappers []string

	// List of loca addresses, one for each reducer. The master will connect to
	// this address on port 22 via SSH and start a local reducer process to
	// listen on this address. The maste rmust be able to connect to this
	// address.
	Reducers []string
}

// FromFile returns a new config read from a JSON file.
func FromFile(file string) (*Config, error) {
	cfg := Config{}
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(raw, &cfg)
	return &cfg, nil
}
