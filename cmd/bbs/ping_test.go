package main_test

import (
	"fmt"
	"net/http"

	"code.cloudfoundry.org/bbs/cmd/bbs/testrunner"
	locketconfig "code.cloudfoundry.org/locket/cmd/locket/config"
	locketrunner "code.cloudfoundry.org/locket/cmd/locket/testrunner"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ping API", func() {
	Describe("Protobuf Ping", func() {
		It("returns true when the bbs is running", func() {
			By("having the bbs down", func() {
				Expect(client.Ping(logger)).To(BeFalse())
			})

			By("starting the bbs without a lock", func() {
				locketPort, err := portAllocator.ClaimPorts(1)
				Expect(err).NotTo(HaveOccurred())

				locketAddress := fmt.Sprintf("localhost:%d", locketPort)
				competingBBSLock := locketrunner.NewLocketRunner(locketBinPath, func(cfg *locketconfig.LocketConfig) {
					cfg.DatabaseConnectionString = sqlRunner.ConnectionString()
					cfg.DatabaseDriver = sqlRunner.DriverName()
					cfg.ListenAddress = locketAddress
				})
				competingBBSLockProcess := ifrit.Invoke(competingBBSLock)
				defer ginkgomon.Kill(competingBBSLockProcess)

				bbsRunner = testrunner.New(bbsBinPath, bbsConfig)
				bbsRunner.StartCheck = "bbs.consul-lock.acquiring-lock"
				bbsProcess = ginkgomon.Invoke(bbsRunner)

				Expect(client.Ping(logger)).To(BeFalse())
			})

			By("finally acquiring the lock", func() {
				Eventually(func() bool {
					return client.Ping(logger)
				}).Should(BeTrue())
			})
		})
	})

	Describe("HTTP Ping", func() {
		It("returns true when the bbs is running", func() {
			var ping = func() bool {
				resp, err := http.Get("http://" + bbsHealthAddress + "/ping")
				if err != nil {
					return false
				}
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return true
				} else {
					return false
				}
			}

			By("having the bbs down", func() {
				Eventually(ping).Should(BeFalse())
			})

			By("starting the bbs without a lock", func() {
				locketPort, err := portAllocator.ClaimPorts(1)
				Expect(err).NotTo(HaveOccurred())

				locketAddress := fmt.Sprintf("localhost:%d", locketPort)
				competingBBSLock := locketrunner.NewLocketRunner(locketBinPath, func(cfg *locketconfig.LocketConfig) {
					cfg.DatabaseConnectionString = sqlRunner.ConnectionString()
					cfg.DatabaseDriver = sqlRunner.DriverName()
					cfg.ListenAddress = locketAddress
				})
				competingBBSLockProcess := ifrit.Invoke(competingBBSLock)
				defer ginkgomon.Kill(competingBBSLockProcess)

				bbsRunner = testrunner.New(bbsBinPath, bbsConfig)
				bbsRunner.StartCheck = "bbs.consul-lock.acquiring-lock"
				bbsProcess = ginkgomon.Invoke(bbsRunner)

				Eventually(ping).Should(BeTrue())
			})
		})
	})
})
