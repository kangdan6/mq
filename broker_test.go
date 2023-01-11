// Copyright 2021 gotomicro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBrokerTopic_NewBroker(t *testing.T) {
	testCases := []struct {
		name       string
		capacity   int
		topics     []string
		wantTopics []string

		wantErr error
	}{
		{
			name:       "topics",
			capacity:   100,
			topics:     []string{"topic1"},
			wantTopics: []string{"topic1"},
		},
		{
			name:     "capacity<=0",
			capacity: -1,
			wantErr:  errors.New("capacity 错误"),
		},
		{
			name:       "empty topics",
			capacity:   100,
			topics:     []string{},
			wantTopics: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBroker(tc.topics, tc.capacity)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			topics, err := b.Topics()
			assert.NoError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantTopics, topics)
		})
	}

}

func TestBrokerTopic_send(t *testing.T) {
	testCases := []struct {
		name          string
		capacity      int
		topics        []string
		consumerTopic string
		timeout       time.Duration
		sendtimes     int

		wantErr error
	}{
		{
			name:          "normal send",
			capacity:      100,
			timeout:       time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			sendtimes:     10,
		},
		{
			name:          "send full",
			capacity:      10,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			timeout:       time.Millisecond,
			sendtimes:     20,

			wantErr: context.DeadlineExceeded,
		},
		{
			name:          "send error topic",
			capacity:      10,
			topics:        []string{"topic1"},
			consumerTopic: "topic-error",

			timeout:   time.Millisecond,
			sendtimes: 20,

			wantErr: errors.New("topic 错误"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBroker(tc.topics, tc.capacity)
			assert.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			// 发送 sendtimes 次消息
			var sendErr error
			for i := 0; i < tc.sendtimes; i++ {
				sendErr = b.Send(ctx, tc.consumerTopic, Msg{content: time.Now().String()})
				if sendErr != nil {
					assert.Equal(t, tc.wantErr, sendErr)
					break
				}

			}

		})
	}

}

func TestBrokerTopic_subscribe(t *testing.T) {
	testCases := []struct {
		name          string
		capacity      int
		topics        []string
		consumerTopic string
		timeout       time.Duration
		sendtimes     int

		wantErr error
	}{
		{
			name:          "normal subscribe",
			capacity:      100,
			timeout:       time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			sendtimes:     1,
		},
		{
			name:          "subscribe error topic",
			capacity:      100,
			timeout:       time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic-error",
			sendtimes:     1,
		},
		{
			name:          "subscribe timeout",
			capacity:      10000,
			timeout:       -time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			sendtimes:     100,
			wantErr:       context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBroker(tc.topics, tc.capacity)
			assert.NoError(t, err)

			go func() {
				msgs, err := b.Subscribe(tc.consumerTopic)
				assert.NoError(t, err)

				for msg := range msgs {
					fmt.Println(msg)
				}
			}()
			// 发送 sendtimes 次消息
			for i := 0; i < tc.sendtimes; i++ {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
					defer cancel()
					b.Send(ctx, tc.consumerTopic, Msg{content: time.Now().String()})
				}()
			}

		})
	}
}

func TestBrokerTopic(t *testing.T) {
	// 并发测试，只是测试有没有死锁之类的问题
	b, err := NewBroker([]string{"topic1"}, 1000)
	assert.NoError(t, err)

	// 模拟发送者
	// 发送 sendtimes 次消息
	for i := 0; i < 1000; i++ {
		go func() {
			b.Send(context.Background(), "topic1", Msg{content: time.Now().String()})
		}()
	}

	// 关闭chan
	go func() {
		time.Sleep(time.Millisecond * 30)
		b.Close()
	}()

	// 模拟消费者
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("消费者 %d", i)
		go func() {
			defer wg.Done()
			msgs, err := b.Subscribe("topic1")
			if err != nil {
				t.Log(err)
				return
			}
			for msg := range msgs {
				fmt.Println(name, msg)
			}
		}()
	}
	wg.Wait()
}
