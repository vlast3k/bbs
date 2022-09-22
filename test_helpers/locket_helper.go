package test_helpers

import (
	"fmt"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/locket"
	"code.cloudfoundry.org/locket/lock"
	locketmodels "code.cloudfoundry.org/locket/models"
	"github.com/tedsuo/ifrit"
)

type LocketHelper struct {
	LocketClient locketmodels.LocketClient
	logger       lager.Logger
}

func NewLocketHelper(logger lager.Logger, LocketClient locketmodels.LocketClient) *LocketHelper {
	return &LocketHelper{
		logger:       logger,
		LocketClient: LocketClient,
	}
}

func (h *LocketHelper) RegisterCell(cell *models.CellPresence) {
	fmt.Println("hello1")
	locketIdentifier := &locketmodels.Resource{
		Key:      "bbs",
		Owner:    "test_helpers",
		Value:    "Something",
		TypeCode: locketmodels.LOCK,
	}
	// Use NewLockRunner instead of NewPresence in order to block on the cell being registered
	runner := lock.NewLockRunner(h.logger, h.LocketClient, locketIdentifier, locket.DefaultSessionTTLInSeconds, clock.NewClock(), locket.RetryInterval)
	fmt.Println("hello2")
	ifrit.Invoke(runner)
	fmt.Println("hello3")
}
