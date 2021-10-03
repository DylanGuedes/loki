package util

import (
	"context"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/dskit/services"
)

const (
	RingKeyOfLeader = 0
)

type ringWatcher struct {
	log           log.Logger
	ring          ring.ReadRing
	notifications util.DNSNotifications
	lookupPeriod  time.Duration
	addressMtx    sync.Mutex
	addresses     []string
}

// NewRingWatcher creates a new Ring watcher and returns a service that is wrapping it.
func NewRingWatcher(log log.Logger, ring ring.ReadRing, lookupPeriod time.Duration, notifications util.DNSNotifications) (services.Service, error) {
	w := &ringWatcher{
		log:           log,
		ring:          ring,
		notifications: notifications,
		lookupPeriod:  lookupPeriod,
	}
	return services.NewBasicService(nil, w.watchLoop, nil), nil
}

// watchLoop watches for changes in DNS and sends notifications.
func (w *ringWatcher) watchLoop(servCtx context.Context) error {

	syncTicker := time.NewTicker(w.lookupPeriod)
	defer syncTicker.Stop()

	for {
		select {
		case <-servCtx.Done():
			return nil
		case <-syncTicker.C:
			w.lookupAddresses()
		}
	}
}

func (w *ringWatcher) lookupAddresses() {

	bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()
	level.Info(w.log).Log("repfactor", w.ring.ReplicationFactor())
	rs, err := w.ring.Get(RingKeyOfLeader, ring.WriteNoExtend, bufDescs, bufHosts, bufZones)
	if err != nil {
		level.Error(w.log).Log("error finding leader at key %d in the ring", RingKeyOfLeader)
		return
	}

	addrs := rs.GetAddresses()

	if len(addrs) == 0 {
		return
	}
	toAdd := make([]string, 0, len(addrs))
	for i, newAddr := range addrs {
		level.Info(w.log).Log("Ring found address: ", newAddr)
		alreadyExists := false
		for _, currAddr := range w.addresses {
			if currAddr == newAddr {
				alreadyExists = true
			}
		}
		if !alreadyExists {
			toAdd = append(toAdd, addrs[i])
		}
	}
	toRemove := make([]string, 0, len(w.addresses))
	for i, existingAddr := range w.addresses {
		stillExists := false
		for _, newAddr := range addrs {
			if newAddr == existingAddr {
				stillExists = true
			}
		}
		if !stillExists {
			toRemove = append(toRemove, w.addresses[i])
		}
	}

	for _, ta := range toAdd {
		level.Info(w.log).Log("Adding: ", ta)
		w.notifications.AddressAdded(ta)
	}

	for _, tr := range toRemove {
		level.Info(w.log).Log("Removing: ", tr)
		w.notifications.AddressRemoved(tr)
	}

	w.addresses = addrs

}
