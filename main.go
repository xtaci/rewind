package main

import (
	"fmt"
	"log"
	"os"

	cli "gopkg.in/urfave/cli.v2"

	"github.com/Shopify/sarama"
	"github.com/jroimartin/gocui"
)

func main() {
	app := &cli.App{
		Name:    "rewind",
		Usage:   `a kafka player like tape control`,
		Version: "1.0",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "brokers, b",
				Value: cli.NewStringSlice("localhost:9092"),
				Usage: "kafka brokers address",
			},
		},
		Action: processor,
	}
	app.Run(os.Args)
}

func processor(c *cli.Context) error {
	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Panicln(err)
	}
	defer g.Close()

	g.SetManagerFunc(layout)

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}

	go play(g, "test", 0, 0)
	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
	return nil
}

func play(g *gocui.Gui, topic string, partition int32, offset int64) {
	client, err := sarama.NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), int64(offset))
	if err != nil {
		panic(err)
	}

	for {
		msg := <-partitionConsumer.Messages()
		g.Execute(func(g *gocui.Gui) error {
			v, err := g.View("content")
			if err != nil {
				return err
				// handle error
			}
			v.Clear()
			fmt.Fprintln(v, string(msg.Value))
			return nil
		})
	}
}

func layout(g *gocui.Gui) error {
	client, err := sarama.NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	maxX, maxY := g.Size()
	if v, err := g.SetView("topics", 0, 0, 10, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "TOPIC"
		topics, err := client.Topics()
		if err != nil {
			panic(err)
		}

		for k := range topics {
			fmt.Fprintln(v, topics[k])
		}
	}

	if v, err := g.SetView("control", 11, maxY-10, maxX-1, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "CTRL"
		fmt.Fprintln(v, "TODO: rewind/replay/fastforward")
	}

	if v, err := g.SetView("content", 11, 0, maxX-1, maxY-11); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "DATA"
	}

	return nil
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}
