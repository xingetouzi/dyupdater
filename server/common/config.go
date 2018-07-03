package common

import (
	"github.com/spf13/viper"
)

// Configable defined the base configable component.
type Configable interface {
	Init(*viper.Viper)
	Close()
}

// Component defined the component which can be enable or disable by config.
type Component interface {
	Configable
	IsEnabled() bool
}

// BaseConfig is the base struct of component's config.
type BaseConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// BaseComponent is base struct of the component.
type BaseComponent struct {
	config *BaseConfig
}

// IsEnabled means whether the component is enabled.
func (c *BaseComponent) IsEnabled() bool {
	return c.config.Enabled
}

// Init the component from config
func (c *BaseComponent) Init(config *viper.Viper) {
	c.config = &BaseConfig{Enabled: true}
	viper.Unmarshal(c.config)
}

// Close the factor source.
func (c *BaseComponent) Close() {
}
