//go:build local_only
// +build local_only

package rabbitmq_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Arten331/observability/logger"
	"github.com/ory/dockertest/v3/docker"
	"github.com/streadway/amqp"
	rabbitmq "gitlab.web-zaim.ru/asterisk/libs/rabbit"
	"gitlab.web-zaim.ru/asterisk/observability/metrics"
	"go.uber.org/zap"
)

type AMQP struct {
	Name              string
	Host              string
	Port              int
	UserName          string
	Password          string
	Vhost             string
	ReconnectAttempts int
	ReconnectInterval int
	Producers         AMPQProducerList
}

type AMPQProducerList struct {
	CTT AMPQProducer
}

type AMPQProducer struct {
	Exchange  string
	QueueName string
}

var ampqConfig = AMQP{ // nolint:gochecknoglobals // test
	Name:              "ctt",
	Host:              "localhost",
	Port:              5672,
	UserName:          "user",
	Password:          "password",
	Vhost:             "test_host",
	ReconnectAttempts: 10,
	ReconnectInterval: 5,
	Producers: AMPQProducerList{
		CTT: AMPQProducer{
			Exchange:  "",
			QueueName: "ctt",
		},
	},
}

func TestProducer_SendMessage(t *testing.T) {
	metricsService := metrics.New()

	ctx, cancel := context.WithCancel(context.Background())

	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	if err = dockerPool.Client.Ping(); err != nil {
		log.Fatalf(`could not connect to docker: %s`, err)
	}

	stdOut := os.Stdout
	rabbitContainer, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rabbitmq",
		Tag:        "3-management-alpine",
		Env: []string{
			fmt.Sprintf("RABBITMQ_DEFAULT_USER=%s", ampqConfig.UserName),
			fmt.Sprintf("RABBITMQ_DEFAULT_PASS=%s", ampqConfig.Password),
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5672/tcp":  {{HostIP: "localhost", HostPort: "5672/tcp"}},
			"15672/tcp": {{HostIP: "localhost", HostPort: "8080/tcp"}},
		},
		ExposedPorts: []string{"5672/tcp"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = rabbitContainer.Close() }()

	<-time.After(time.Second * 15)

	cmd := [][]string{
		{"rabbitmqctl", fmt.Sprintf("add_vhost %s", ampqConfig.Vhost)},
		{"rabbitmqctl", fmt.Sprintf("set_user_tags %s administrator", ampqConfig.UserName)},
		{"rabbitmqctl", fmt.Sprintf(`set_permissions -p test_host %s .* .* .*`, ampqConfig.UserName)},
		{"rabbitmqadmin", fmt.Sprintf("-V %s --username %s --password %s declare queue name=%s",
			ampqConfig.Vhost, ampqConfig.UserName, ampqConfig.Password, ampqConfig.Producers.CTT.QueueName)},
	}

	for _, c := range cmd {
		comm := append([]string{c[0]}, strings.Split(c[1], " ")...)

		logger.L().Info("", zap.Strings("cmd", c))

		_, err := rabbitContainer.Exec(comm, dockertest.ExecOptions{
			StdOut: stdOut,
			StdErr: stdOut,
		})
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	rabbitmqService := rabbitmq.New(&rabbitmq.Config{
		Schema:              "amqp",
		Username:            ampqConfig.UserName,
		Password:            ampqConfig.Password,
		Host:                ampqConfig.Host,
		Port:                strconv.Itoa(ampqConfig.Port),
		Vhost:               ampqConfig.Vhost,
		ConnectionName:      "cdr_to_stt",
		ReconnectInterval:   time.Duration(ampqConfig.ReconnectInterval) * time.Second,
		ReconnectMaxAttempt: ampqConfig.ReconnectAttempts,
	}, "cdr_to_stt")

	err = metricsService.RegisterService(rabbitmqService)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err := http.ListenAndServe(":8098", metricsService.Handler())
		if err != nil {
			log.Fatalln(err)
		}
	}()

	testExchange, _ := rabbitmq.NewExchange("", "direct", true, false, false, false)

	testProducer, err := rabbitmq.NewProducer(rabbitmq.ProducerOptions{
		Exchange:    &testExchange,
		ConfirmMode: true,
	}, rabbitmqService)
	if err != nil {
		t.Fatal(err)
	}

	rabbitmqService.RegisterProducer(testProducer)

	rabbitmqService.Run(ctx, cancel)

	go func() {
		for i := 0; i < 10000; i++ {
			<-time.After(time.Millisecond * 2)
			logger.L().Info("last msg", zap.Int("i", i))

			msg := rabbitmq.NewMessage(
				[]byte(strconv.Itoa(i)),
				amqp.Persistent,
				"",
				ampqConfig.Producers.CTT.QueueName,
				ampqConfig.Producers.CTT.Exchange,
			)

			err := testProducer.SendMessage(&msg)
			if err != nil {
				t.Error(err)
			}
		}
	}()

	sig := make(chan os.Signal, 1)

	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-time.After(time.Second * 5)
		sig <- syscall.SIGQUIT
	}()

	go func() {
		<-sig

		shutdownCtx, stop := context.WithTimeout(ctx, 30*time.Second)

		go func() {
			<-shutdownCtx.Done()

			if shutdownCtx.Err() == context.DeadlineExceeded {
				log.Panicf("graceful shutdown timed out.. forcing exit.")
			}
		}()

		err := rabbitmqService.Shutdown(ctx)
		if err != nil {
			log.Fatalln(err)
		}

		stop()
		cancel()
	}()

	<-ctx.Done()
}
