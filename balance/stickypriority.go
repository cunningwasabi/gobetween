/**
 * StickyPriority.go - priority based "sticky session" balance implementation
 * allow for a "preferred backend" for new sessions, whilst keeping old session on existing backends
 * uses 'priority' balancer for initial backend selection
 *
 * @author quedunk <quedunk@gmail.com>
 */

package balance

import (
	"errors"
	"time"

	"github.com/yyyar/gobetween/config"
	"github.com/yyyar/gobetween/core"
	"github.com/yyyar/gobetween/logging"
)

/**
 * balancer implements "sticky" priority based balancing.
 */
type StickyPriorityBalancer struct {
	duration time.Duration

	/* sticky table mapping */
	/* ip str -> session */
	table map[string]*StickyPrioritySession
}

/**
 * StickyPriority balancing session
 */
type StickyPrioritySession struct {
	backend   *core.Backend
	timer     *time.Timer
	lasttouch time.Time
}

/**
 * Constructor
 */
func NewStickyPriorityBalancer(cfg config.BalanceConfig) interface{} {

	b := &StickyPriorityBalancer{
		table: map[string]*StickyPrioritySession{},
	}

	b.duration, _ = time.ParseDuration(cfg.StickyPrioritySessionIdleExpiry)

	return b
}

/**
 * Elect backend using priority strategy
 * It keeps mapping cache for some period of time.
 */
func (b *StickyPriorityBalancer) Elect(context core.Context, backends []*core.Backend) (*core.Backend, error) {
	log := logging.For("balance/StickyPriority")

	if len(backends) == 0 {
		return nil, errors.New("Can't elect backend, Backends empty")
	}

	var backend *core.Backend
	var err error
	sess, ok := b.table[context.Ip().String()]
	if !ok {
		// we couldnt find an existing session;
		// - make one + give it a valid backend
		// - set up a timer to clean up once idle expiry time has been reached

		backend, err = ((*PriorityBalancer)(nil)).Elect(context, backends)
		b.table[context.Ip().String()] = &StickyPrioritySession{
			backend: backend,
		}

		sess = b.table[context.Ip().String()]

		// touch the session
		sess.lasttouch = time.Now()

		// set the timer going
		setTimer(context, *b)

		log.Info("client ", context.Ip(), " new session on backend ", sess.backend.Address())

	} else {
		// got a session, check if previously elected backend is valid
		for _, validbackend := range backends {
			if validbackend.Address() == sess.backend.Address() {
				backend = validbackend
				// if the backend has been flagged to drain sessions, then we stop updating the
				// 'lasttouch' for that guy and let the session expiry normally.
				if backend.DrainSessions != true {
					sess.lasttouch = time.Now()
				}
				break
			}
		}
		// couldnt find the old backend? get a new one!
		if backend == nil {
			backend, err = ((*PriorityBalancer)(nil)).Elect(context, backends)
			log.Debug("client ", context.Ip(), " existing backend not valid, selected new one ", sess.backend.Address())
			sess.backend = backend
			sess.lasttouch = time.Now()
		}
	}

	return backend, err
}

func setTimer(context core.Context, b StickyPriorityBalancer) {
	log := logging.For("balance/StickyPriority/setTimer")

	log.Debug("client ", context.Ip().String(), " setting expirycheck timer")

	sess := b.table[context.Ip().String()]
	// expiry seconds is; lasttouch + duration of expiry - timenow.
	expirysecs := sess.lasttouch.Add(b.duration).Sub(time.Now())

	// if expirysecs < 0, then afterfunc will ignore it (accoring to sleep.go doco)
	sess.timer = time.AfterFunc(expirysecs, func() {
		// wait for the timer to expiry, then do this to see if we need to clean up:
		log.Info("client ", context.Ip().String(), " expirytimer - triggered")
		sess := b.table[context.Ip().String()]
		if sess != nil {
			log.Info("client ", context.Ip().String(), " expirytimer - found existing session")
			if time.Now().After(sess.lasttouch.Add(b.duration)) {
				log.Info("client ", context.Ip().String(), " expirytimer - session expired")
				delete(b.table, context.Ip().String())
				log.Info("client ", context.Ip().String(), " expirytimer - session deleted")
			} else {
				log.Info("client ", context.Ip().String(), " expirytimer - session not expired, setting new timer")
				setTimer(context, b)
			}
		} else {
			log.Info("client ", context.Ip().String(), " expirytimer - session not found")
		}
	})
}
