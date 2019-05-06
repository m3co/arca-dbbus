package dbbus

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"

	jsonrpc "github.com/m3co/arca-jsonrpc"
)

// setupListenNotify whatever
func (s *Server) setupListenNotify(connStr string) {
	listener := pq.NewListener(connStr,
		time.Second*2, time.Minute*5,
		func(_ pq.ListenerEventType, err error) {
			if err != nil {
				fmt.Println(err.Error())
			}
		})

	if err := listener.Listen("jsonrpc"); err != nil {
		panic(err)
	}

	for {
		msg, ok := <-listener.Notify
		if !ok {
			log.Println("Disconnected")
			return
		}
		var notification Notification
		err := json.Unmarshal([]byte(msg.Extra), &notification)
		if err != nil {
			panic(err)
		}

		iPrime := notification.Context["Prime"]
		iNotification := notification.Context["Notification"]

		var isprime bool
		if iPrime != nil {
			isprime = iPrime.(bool)
		}
		var isnotification bool
		if iNotification != nil {
			isnotification = iNotification.(bool)
		}

		if isprime {
			request := jsonrpc.Request{}
			request.Method = notification.Method
			request.Context = notification.Context
			request.Params = notification.Result
			for _, db := range s.dbs {
				response, err := s.rpc.ProcessNotification(&request, db)
				if err != nil {
					msg, _ := json.Marshal(response)
					s.rpc.Broadcast(msg)
				}
			}
		}
		if isnotification {
			s.rpc.Broadcast([]byte(msg.Extra))
		}
	}
}
