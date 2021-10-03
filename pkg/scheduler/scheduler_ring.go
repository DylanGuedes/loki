package scheduler

import (
	"flag"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"

	"github.com/cortexproject/cortex/pkg/ring"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const (
	// RingKey is the key under which we store the store gateways ring in the KVStore.
	RingKey = "scheduler"

	// RingNameForServer is the name of the ring used by the store gateway server.
	RingNameForServer = "scheduler"

	// RingNameForClient is the name of the ring used by the store gateway client (we need
	// a different name to avoid clashing Prometheus metrics when running in single-binary).
	RingNameForClient = "scheduler-client"

	// We use a safe default instead of exposing to config option to the user
	// in order to simplify the config.
	RingNumTokens = 512
)

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the distributors ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	KVStore              kv.Config     `yaml:"kvstore"`
	HeartbeatPeriod      time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout     time.Duration `yaml:"heartbeat_timeout"`
	TokensFilePath       string        `yaml:"tokens_file_path"`
	ZoneAwarenessEnabled bool          `yaml:"zone_awareness_enabled"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"hidden"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names"`
	InstancePort           int      `yaml:"instance_port" doc:"hidden"`
	InstanceAddr           string   `yaml:"instance_addr" doc:"hidden"`
	InstanceZone           string   `yaml:"instance_availability_zone"`

	// Injected internally
	ListenPort int `yaml:"-"`

	ObservePeriod time.Duration `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("scheduler.ring.", "schedulers/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "scheduler.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "scheduler.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which schedulers are considered unhealthy within the ring. 0 = never (timeout disabled).")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "scheduler.ring.instance-interface-names", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "scheduler.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "scheduler.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "scheduler.ring.instance-id", hostname, "Instance ID to register in the ring.")
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the scheduler
// ring config.
//func (cfg *RingConfig) ToLifecyclerConfig() (ring.BasicLifecyclerConfig, error) {
//	instanceAddr, err := ring.GetInstanceAddr(cfg.InstanceAddr, cfg.InstanceInterfaceNames)
//	if err != nil {
//		return ring.BasicLifecyclerConfig{}, err
//	}
//
//	instancePort := ring.GetInstancePort(cfg.InstancePort, cfg.ListenPort)
//
//	return ring.BasicLifecyclerConfig{
//		ID:                  cfg.InstanceID,
//		Addr:                fmt.Sprintf("%s:%d", instanceAddr, instancePort),
//		Zone:                cfg.InstanceZone,
//		HeartbeatPeriod:     cfg.HeartbeatPeriod,
//		TokensObservePeriod: 0,
//		NumTokens:           RingNumTokens,
//	}, nil
//}

// ToLifecyclerConfig returns a LifecyclerConfig based on the compactor
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig() ring.LifecyclerConfig {
	// We have to make sure that the ring.LifecyclerConfig and ring.Config
	// defaults are preserved
	lc := ring.LifecyclerConfig{}
	rc := ring.Config{}

	flagext.DefaultValues(&lc)
	flagext.DefaultValues(&rc)

	// Configure ring
	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	// We want 2 schedulers so we leverage replication factor to return 2 schedulers from ring Get calls
	rc.ReplicationFactor = 2

	// Configure lifecycler
	lc.RingConfig = rc
	lc.RingConfig.SubringCacheDisabled = true
	lc.ListenPort = cfg.ListenPort
	lc.Addr = cfg.InstanceAddr
	lc.Port = cfg.InstancePort
	lc.ID = cfg.InstanceID
	lc.InfNames = cfg.InstanceInterfaceNames
	lc.UnregisterOnShutdown = true
	lc.HeartbeatPeriod = cfg.HeartbeatPeriod
	lc.ObservePeriod = cfg.ObservePeriod
	lc.JoinAfter = 0
	lc.MinReadyDuration = 0
	lc.FinalSleep = 0

	// We use a safe default instead of exposing to config option to the user
	// in order to simplify the config.
	lc.NumTokens = RingNumTokens

	return lc
}

func (cfg *RingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	rc.ReplicationFactor = 2

	return rc
}
