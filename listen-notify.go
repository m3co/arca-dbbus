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

		/*
			Expliquemos aquí el significado de (isprime): Cuando una vista primaria
			pretende afectar a una tabla primaria, tenemos que ocurre un JSON-RPC con
			el campo "Prime": true. En éste caso, hay que ejecutar dicho "request" en
			todas las bases de datos para así garantizar consistencia en todas partes.
		*/
		if isprime {
			request := jsonrpc.Request{}
			request.Method = notification.Method
			request.Context = notification.Context
			request.Params = notification.Result
			for _, db := range s.dbs {
				s.rpc.ProcessNotification(&request, db)
			}
			continue // favor, salir del ciclo presente*
		}

		/*
			Cualquier notificación, sin importar su carácter, hay que notificarla a
			todos los clientes.
		*/
		if isnotification {
			s.rpc.Broadcast([]byte(msg.Extra))
			continue // favor, salir del ciclo presente*
		}

		/*
			favor, salir del ciclo presente* : es porque no se aceptan notificaciones
			con diferentes intenciones.
			Si la notificación contiene "Prime": true entonces "Notification": true
			no puede estar presente al mismo tiempo, y viceversa.
		*/

		/*
			Este caso ocurre cuando no es ni notificación ni es a primaria.
			Es decir, en este caso ocurre que la notificación es para ejecutar el RPC
			sobre una vista determinada.
		*/
	}
}
