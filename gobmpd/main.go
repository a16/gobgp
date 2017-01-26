// Copyright (C) 2015 Nippon Telegraph and Telephone Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"net"
	"os"
	"runtime"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/osrg/gobgp/packet/bgp"
	"github.com/osrg/gobgp/packet/bmp"
	"github.com/osrg/gobgp/table"
)

func handleBGPUpdate(bph *bmp.BMPPeerHeader, bgpMsg *bgp.BGPMessage) []*table.Path {
	peerInfo := &table.PeerInfo{
		Address: bph.PeerAddress,
		AS:      bph.PeerAS,
		ID:      bph.PeerBGPID,
	}
	ts := time.Unix(int64(bph.Timestamp), int64(0))
	return table.ProcessMessage(bgpMsg, peerInfo, ts)
}

func connLoop(conn *net.TCPConn) {
	addr := conn.RemoteAddr()
	scanner := bufio.NewScanner(bufio.NewReader(conn))
	scanner.Split(bmp.SplitBMP)

	for scanner.Scan() {
		buf := scanner.Bytes()
		bmpMsg, err := bmp.ParseBMPMessage(buf)
		if err != nil {
			continue
		}

		peerAddr := make(net.IP, len(bmpMsg.PeerHeader.PeerAddress))
		copy(peerAddr, bmpMsg.PeerHeader.PeerAddress)
		peerAS := bmpMsg.PeerHeader.PeerAS
		switch bmpMsg.Header.Type {
		case bmp.BMP_MSG_ROUTE_MONITORING:
			bmpRouteMonitoringMsg := bmpMsg.Body.(*bmp.BMPRouteMonitoring)
			for _, p := range handleBGPUpdate(&bmpMsg.PeerHeader, bmpRouteMonitoringMsg.BGPUpdate) {
				if p == nil || p.IsEOR() {
					log.WithFields(log.Fields{
						"Topic": "BMP",
					}).Infof("Received EoR from %v, %v, %v, %#v", conn.RemoteAddr(), peerAS, peerAddr, p)
					continue
				}
			}
		}
	}
	log.Info("conn was closed ", addr)
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFormatter(&log.JSONFormatter{
		TimestampFormat: "2006/01/02 15:04:05",
	})
	log.SetLevel(log.InfoLevel)
}

func main() {
	service := ":" + strconv.Itoa(bmp.BMP_DEFAULT_PORT)
	addr, _ := net.ResolveTCPAddr("tcp", service)

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Info(err)
		os.Exit(1)
	}

	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Info(err)
			continue
		}
		log.Info("Accepted a new connection from ", conn.RemoteAddr())

		go connLoop(conn)
	}
}
