package main

import (
	"log"
	"math/rand"
	"strconv"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	args := make(amqp.Table)
	args["x-max-priority"] = int64(30)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"test", // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		args,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 30 Messages with random Priorities between 0 and 9
	for msgs := 0; msgs < 100; msgs++ {
		prio := rand.Intn(31)
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            []byte(strconv.Itoa(prio)),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        uint8(prio),
			})
		log.Printf(" [x] Sent prio %d", prio)
		failOnError(err, "Failed to publish a message")

	}
}
